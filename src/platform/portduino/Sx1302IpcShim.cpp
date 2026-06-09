#if defined(ARCH_PORTDUINO) && defined(SX1302_NATIVE_IPC_SHIM_ENABLE) && defined(__linux__)

#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

/* C++ includes for mesh packet delivery */
#include "mesh/RadioInterface.h"
#include "mesh/Router.h"
#include "mesh/MeshTypes.h"
#include "concurrency/NotifiedWorkerThread.h"
#include <atomic>
#include <mutex>
#include <queue>

#define MTK_IPC_MAGIC0 'M'
#define MTK_IPC_MAGIC1 'T'
#define MTK_IPC_MAGIC2 'K'
#define MTK_IPC_MAGIC3 '1'

#define MTK_IPC_VERSION 1

#define MTK_IPC_TYPE_HELLO 1
#define MTK_IPC_TYPE_UPLINK 2
#define MTK_IPC_TYPE_DOWNLINK 3
#define MTK_IPC_TYPE_APP_SEND 4

#define MTK_IPC_MAX_FRAME 2048
#define MTK_IPC_DEFAULT_SOCKET_PATH "/tmp/meshtastic-sx1302.sock"

static pthread_once_t g_start_once = PTHREAD_ONCE_INIT;
static pthread_t g_thread;

/* -------- Thread-safe uplink delivery into the mesh router -------- */

struct ShimRxFrame {
    float    snr;
    int16_t  rssi;
    uint16_t data_len;
    uint8_t  data[MAX_LORA_PAYLOAD_LEN];
};

static std::mutex              g_rx_mutex;
static std::queue<ShimRxFrame> g_rx_queue;

namespace {

class ShimDeliveryThread : public concurrency::NotifiedWorkerThread
{
  public:
    ShimDeliveryThread() : concurrency::NotifiedWorkerThread("ShimRx") {}

  protected:
    void onNotify(uint32_t /*notification*/) override
    {
        for (;;) {
            ShimRxFrame frame;
            {
                std::lock_guard<std::mutex> lk(g_rx_mutex);
                if (g_rx_queue.empty()) break;
                frame = g_rx_queue.front();
                g_rx_queue.pop();
            }
            deliverFrame(frame);
        }
    }

  private:
    static void deliverFrame(const ShimRxFrame &f)
    {
        if (f.data_len < sizeof(PacketHeader)) {
            fprintf(stderr, "[MTK_NATIVE_IPC] frame too short (%u), dropping\n", f.data_len);
            return;
        }
        const PacketHeader *hdr = reinterpret_cast<const PacketHeader *>(f.data);
        if (hdr->from == 0) return;

        int32_t enc_len = (int32_t)f.data_len - (int32_t)sizeof(PacketHeader);
        if (enc_len < 0) return;

        meshtastic_MeshPacket *mp = packetPool.allocZeroed();
        if (!mp) {
            fprintf(stderr, "[MTK_NATIVE_IPC] packetPool exhausted, dropping\n");
            return;
        }

        mp->from       = hdr->from;
        mp->to         = hdr->to;
        mp->id         = hdr->id;
        mp->channel    = hdr->channel;
        mp->hop_limit  = hdr->flags & PACKET_FLAGS_HOP_LIMIT_MASK;
        mp->hop_start  = (hdr->flags & PACKET_FLAGS_HOP_START_MASK) >> PACKET_FLAGS_HOP_START_SHIFT;
        mp->want_ack   = !!(hdr->flags & PACKET_FLAGS_WANT_ACK_MASK);
        mp->via_mqtt   = !!(hdr->flags & PACKET_FLAGS_VIA_MQTT_MASK);
        mp->next_hop   = (mp->hop_start == 0) ? NO_NEXT_HOP_PREFERENCE : hdr->next_hop;
        mp->relay_node = (mp->hop_start == 0) ? NO_RELAY_NODE       : hdr->relay_node;
        mp->rx_snr     = f.snr;
        mp->rx_rssi    = (int32_t)f.rssi;
        mp->rx_time    = (uint32_t)time(NULL);
        mp->which_payload_variant = meshtastic_MeshPacket_encrypted_tag;
        mp->transport_mechanism   = meshtastic_MeshPacket_TransportMechanism_TRANSPORT_LORA;

        if (enc_len > 0) {
            if ((uint32_t)enc_len > sizeof(mp->encrypted.bytes)) {
                fprintf(stderr, "[MTK_NATIVE_IPC] enc payload too large (%d), dropping\n", enc_len);
                packetPool.release(mp);
                return;
            }
            memcpy(mp->encrypted.bytes, f.data + sizeof(PacketHeader), enc_len);
            mp->encrypted.size = (uint32_t)enc_len;
        }

        if (router) {
            fprintf(stderr, "[MTK_NATIVE_IPC] deliver from=0x%08x to=0x%08x id=0x%08x enc=%d\n",
                    mp->from, mp->to, mp->id, enc_len);
            router->enqueueReceivedMessage(mp);
        } else {
            fprintf(stderr, "[MTK_NATIVE_IPC] router not ready, dropping\n");
            packetPool.release(mp);
        }
    }
};

} // namespace

static ShimDeliveryThread *g_delivery_thread = nullptr;
static std::atomic<int> g_client_fd{-1};

static void enqueue_shim_rx(const uint8_t *data, uint16_t data_len, float snr, int16_t rssi)
{
    if (data_len == 0 || data_len > MAX_LORA_PAYLOAD_LEN) return;
    ShimRxFrame frame;
    frame.snr      = snr;
    frame.rssi     = rssi;
    frame.data_len = data_len;
    memcpy(frame.data, data, data_len);
    { std::lock_guard<std::mutex> lk(g_rx_mutex); g_rx_queue.push(frame); }
    if (g_delivery_thread) g_delivery_thread->notify(1, false);
}

static uint16_t read_u16le(const uint8_t *p)
{
    return (uint16_t)p[0] | ((uint16_t)p[1] << 8);
}

static uint32_t read_u32le(const uint8_t *p)
{
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static int16_t read_i16le(const uint8_t *p)
{
    return (int16_t)read_u16le(p);
}

static float read_floatle(const uint8_t *p)
{
    float out;
    uint32_t u = read_u32le(p);
    memcpy(&out, &u, sizeof(out));
    return out;
}

static int append_u8(uint8_t *buf, size_t cap, size_t *off, uint8_t v)
{
    if ((*off + 1U) > cap) {
        return -1;
    }
    buf[*off] = v;
    *off += 1U;
    return 0;
}

static int append_u16le(uint8_t *buf, size_t cap, size_t *off, uint16_t v)
{
    if ((*off + 2U) > cap) {
        return -1;
    }
    buf[*off + 0U] = (uint8_t)(v & 0xFFU);
    buf[*off + 1U] = (uint8_t)((v >> 8) & 0xFFU);
    *off += 2U;
    return 0;
}

static int append_u32le(uint8_t *buf, size_t cap, size_t *off, uint32_t v)
{
    if ((*off + 4U) > cap) {
        return -1;
    }
    buf[*off + 0U] = (uint8_t)(v & 0xFFU);
    buf[*off + 1U] = (uint8_t)((v >> 8) & 0xFFU);
    buf[*off + 2U] = (uint8_t)((v >> 16) & 0xFFU);
    buf[*off + 3U] = (uint8_t)((v >> 24) & 0xFFU);
    *off += 4U;
    return 0;
}

static int append_i16le(uint8_t *buf, size_t cap, size_t *off, int16_t v)
{
    return append_u16le(buf, cap, off, (uint16_t)v);
}

static int send_frame(int fd, uint8_t type, const uint8_t *payload, uint16_t payload_len)
{
    uint8_t frame[MTK_IPC_MAX_FRAME];
    size_t off = 0;
    ssize_t n;

    if ((size_t)(8U + payload_len) > sizeof(frame)) {
        return -1;
    }

    if (append_u8(frame, sizeof(frame), &off, MTK_IPC_MAGIC0) != 0 ||
        append_u8(frame, sizeof(frame), &off, MTK_IPC_MAGIC1) != 0 ||
        append_u8(frame, sizeof(frame), &off, MTK_IPC_MAGIC2) != 0 ||
        append_u8(frame, sizeof(frame), &off, MTK_IPC_MAGIC3) != 0 ||
        append_u8(frame, sizeof(frame), &off, MTK_IPC_VERSION) != 0 ||
        append_u8(frame, sizeof(frame), &off, type) != 0 ||
        append_u16le(frame, sizeof(frame), &off, payload_len) != 0) {
        return -1;
    }

    if ((payload != NULL) && (payload_len > 0U)) {
        memcpy(frame + off, payload, payload_len);
        off += payload_len;
    }

    n = send(fd, frame, off, MSG_NOSIGNAL);
    if (n < 0) {
        return -1;
    }
    return ((size_t)n == off) ? 0 : -1;
}

static int send_echo_downlink(
    int fd,
    const uint8_t *payload,
    uint16_t payload_len,
    uint32_t freq_hz,
    uint32_t tmst,
    uint32_t bw_hz,
    uint8_t sf,
    uint8_t cr,
    uint8_t rf_chain)
{
    uint8_t msg[1300];
    size_t off = 0;

    if ((size_t)(19U + payload_len) > sizeof(msg)) {
        return -1;
    }

    if (append_u32le(msg, sizeof(msg), &off, freq_hz) != 0 ||
        append_u32le(msg, sizeof(msg), &off, tmst + 200000U) != 0 ||
        append_i16le(msg, sizeof(msg), &off, 14) != 0 ||
        append_u32le(msg, sizeof(msg), &off, bw_hz) != 0 ||
        append_u8(msg, sizeof(msg), &off, sf) != 0 ||
        append_u8(msg, sizeof(msg), &off, cr) != 0 ||
        append_u8(msg, sizeof(msg), &off, rf_chain) != 0 ||
        append_u16le(msg, sizeof(msg), &off, payload_len) != 0) {
        return -1;
    }

    memcpy(msg + off, payload, payload_len);
    off += payload_len;

    return send_frame(fd, MTK_IPC_TYPE_DOWNLINK, msg, (uint16_t)off);
}

extern "C" void sx1302_ipc_shim_tx(const uint8_t *buf, size_t len, uint32_t freq_hz,
                                    uint8_t sf, uint32_t bw_hz, uint8_t cr, int8_t tx_power_dbm)
{
    uint8_t msg[MTK_IPC_MAX_FRAME];
    size_t off = 0;

    if (len == 0 || len > 255U) return;

    int fd = g_client_fd.load(std::memory_order_relaxed);
    if (fd < 0) {
        fprintf(stderr, "[MTK_NATIVE_IPC] TX: no client connected, dropping\n");
        return;
    }

    if (append_u32le(msg, sizeof(msg), &off, freq_hz) != 0 ||
        append_u32le(msg, sizeof(msg), &off, 0U) != 0 ||
        append_i16le(msg, sizeof(msg), &off, (int16_t)tx_power_dbm) != 0 ||
        append_u32le(msg, sizeof(msg), &off, bw_hz) != 0 ||
        append_u8(msg, sizeof(msg), &off, sf) != 0 ||
        append_u8(msg, sizeof(msg), &off, cr) != 0 ||
        append_u8(msg, sizeof(msg), &off, 0U) != 0 ||
        append_u16le(msg, sizeof(msg), &off, (uint16_t)len) != 0) {
        return;
    }

    if ((off + len) > sizeof(msg)) return;
    memcpy(msg + off, buf, len);
    off += len;

    fprintf(stderr, "[MTK_NATIVE_IPC] TX freq=%" PRIu32 " sf=%u bw=%" PRIu32 " len=%zu\n",
            freq_hz, (unsigned)sf, bw_hz, len);
    (void)send_frame(fd, MTK_IPC_TYPE_DOWNLINK, msg, (uint16_t)off);
}

static void *shim_thread_main(void *arg)
{
    const char *socket_path = (const char *)arg;
    const char *echo_env = getenv("MTK_STUB_ECHO_DOWNLINK");
    bool echo_downlink = (echo_env != NULL) && (strcmp(echo_env, "1") == 0);
    int listen_fd = -1;
    int client_fd = -1;
    struct sockaddr_un addr;

    listen_fd = socket(AF_UNIX, SOCK_SEQPACKET, 0);
    if (listen_fd < 0) {
        fprintf(stderr, "[MTK_NATIVE_IPC] socket failed: %s\n", strerror(errno));
        return NULL;
    }

    unlink(socket_path);
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, socket_path, sizeof(addr.sun_path) - 1);

    if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        fprintf(stderr, "[MTK_NATIVE_IPC] bind failed on %s: %s\n", socket_path, strerror(errno));
        close(listen_fd);
        return NULL;
    }

    if (listen(listen_fd, 1) != 0) {
        fprintf(stderr, "[MTK_NATIVE_IPC] listen failed: %s\n", strerror(errno));
        close(listen_fd);
        unlink(socket_path);
        return NULL;
    }

    fprintf(stderr, "[MTK_NATIVE_IPC] listening on %s\n", socket_path);

    for (;;) {
        uint8_t frame[MTK_IPC_MAX_FRAME];
        ssize_t n;

        if (client_fd < 0) {
            client_fd = accept(listen_fd, NULL, NULL);
            if (client_fd < 0) {
                if (errno == EINTR) {
                    continue;
                }
                fprintf(stderr, "[MTK_NATIVE_IPC] accept failed: %s\n", strerror(errno));
                continue;
            }
            fprintf(stderr, "[MTK_NATIVE_IPC] client connected\n");
            g_client_fd.store(client_fd, std::memory_order_relaxed);
        }

        n = recv(client_fd, frame, sizeof(frame), 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            fprintf(stderr, "[MTK_NATIVE_IPC] recv failed: %s\n", strerror(errno));
            g_client_fd.store(-1, std::memory_order_relaxed);
            close(client_fd);
            client_fd = -1;
            continue;
        }

        if (n == 0) {
            fprintf(stderr, "[MTK_NATIVE_IPC] client disconnected\n");
            g_client_fd.store(-1, std::memory_order_relaxed);
            close(client_fd);
            client_fd = -1;
            continue;
        }

        if (n < 8) {
            continue;
        }
        if ((frame[0] != MTK_IPC_MAGIC0) || (frame[1] != MTK_IPC_MAGIC1) || (frame[2] != MTK_IPC_MAGIC2) ||
            (frame[3] != MTK_IPC_MAGIC3) || (frame[4] != MTK_IPC_VERSION)) {
            continue;
        }

        {
            uint8_t type = frame[5];
            uint16_t payload_len = read_u16le(frame + 6);
            const uint8_t *payload = frame + 8;

            if ((size_t)(8U + payload_len) != (size_t)n) {
                continue;
            }

            if (type == MTK_IPC_TYPE_HELLO) {
                fprintf(stderr, "[MTK_NATIVE_IPC] HELLO: %.*s\n", payload_len, (const char *)payload);
            } else if (type == MTK_IPC_TYPE_UPLINK) {
                if (payload_len >= 23U) {
                    uint32_t freq_hz = read_u32le(payload + 0);
                    uint32_t tmst = read_u32le(payload + 4);
                    int16_t rssi = read_i16le(payload + 8);
                    float snr = read_floatle(payload + 10);
                    uint32_t bw_hz = read_u32le(payload + 14);
                    uint8_t sf = payload[18];
                    uint8_t cr = payload[19];
                    uint8_t rf_chain = payload[20];
                    uint16_t data_len = read_u16le(payload + 21);
                    const uint8_t *data = payload + 23;

                    if ((uint16_t)(23U + data_len) == payload_len) {
                        fprintf(stderr,
                                "[MTK_NATIVE_IPC] UPLINK freq=%" PRIu32 " sf=%u bw=%" PRIu32 " cr=%u rssi=%d snr=%.2f len=%u\n",
                                freq_hz, sf, bw_hz, cr, rssi, snr, data_len);
                        enqueue_shim_rx(data, data_len, snr, rssi);
                        if (echo_downlink && send_echo_downlink(client_fd, data, data_len, freq_hz, tmst, bw_hz, sf, cr, rf_chain) == 0) {
                            fprintf(stderr, "[MTK_NATIVE_IPC] DOWNLINK echo queued len=%u\n", data_len);
                        }
                    }
                }
            } else if (type == MTK_IPC_TYPE_APP_SEND) {
                if (payload_len >= 4U) {
                    uint8_t topic = payload[0];
                    uint8_t target_len = payload[1];
                    uint16_t data_len = read_u16le(payload + 2);
                    if ((uint16_t)(4U + target_len + data_len) == payload_len) {
                        fprintf(stderr, "[MTK_NATIVE_IPC] APP_SEND topic=%u target_len=%u payload_len=%u\n", topic, target_len,
                                data_len);
                    }
                }
            }
        }
    }

    return NULL;
}

static void start_once(void)
{
    const char *socket_path = getenv("MESHTASTIC_IPC_SOCKET");
    static char socket_buf[sizeof(((struct sockaddr_un *)0)->sun_path)] = {0};

    if ((socket_path == NULL) || (socket_path[0] == '\0')) {
        socket_path = MTK_IPC_DEFAULT_SOCKET_PATH;
    }

    strncpy(socket_buf, socket_path, sizeof(socket_buf) - 1);
    socket_buf[sizeof(socket_buf) - 1] = '\0';

    g_delivery_thread = new ShimDeliveryThread();

    if (pthread_create(&g_thread, NULL, shim_thread_main, socket_buf) != 0) {
        fprintf(stderr, "[MTK_NATIVE_IPC] failed to start thread\n");
        return;
    }
    pthread_detach(g_thread);
}

extern "C" void sx1302_ipc_shim_start(void)
{
    pthread_once(&g_start_once, start_once);
}

#else

extern "C" void sx1302_ipc_shim_start(void)
{
}

#endif