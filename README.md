# vmq_kafka_plugin

**vmq_kafka_plugin** is a [VerneMQ](https://vernemq.com/) plugin that routes MQTT messages to Apache Kafka topics, with support for **configurable static Kafka headers** via reusable header profiles.

---

## 📜 About VerneMQ

[VerneMQ](https://vernemq.com/) is an open-source, high-performance, and scalable MQTT broker written in Erlang/OTP.  
It supports MQTT v3.1, v3.1.1, and v5, TLS-secured connections, distributed clustering, and message persistence.  
VerneMQ is designed to handle millions of concurrent connections and integrate seamlessly into IoT and distributed messaging architectures.

**Useful references:**
- Website: [https://vernemq.com/](https://vernemq.com/)
- Documentation: [https://docs.vernemq.com/](https://docs.vernemq.com/)
- GitHub repository: [https://github.com/vernemq/vernemq](https://github.com/vernemq/vernemq)

---

## 🔌 Plugin Overview

`vmq_kafka_plugin` enables:
- Receiving MQTT messages in VerneMQ
- Routing them to one or more Kafka topics
- Attaching **static Kafka headers** based on the originating MQTT topic pattern, using reusable header profiles

### How It Works

1. **Configuration**  
   In `advanced.config`, you define:
   - `headers_profiles`: sets of key/value pairs to be used as Kafka headers
   - `mappings`: MQTT → Kafka routing rules, each linked to a single header profile

2. **Pattern Matching**  
   The plugin precompiles MQTT topic filters into regular expressions and matches incoming messages against them.

3. **Kafka Publishing**  
   The worker sends the payload to the configured Kafka topic, attaching the headers from the associated profile.

---

## ⚙️ Configuration File Structure (`advanced.config`)

The plugin reads its configuration from VerneMQ’s `advanced.config`.  
Below is a complete example:

```erlang
[
  {vmq_kafka_plugin, [

     {brokers, [
        {"localhost", 9092}
     ]},

     {client_opts, [
        {client_id_base,     "vmq_kafka"},
        {request_timeout_ms, 15000}
     ]},

     {fast_opts, [
        {required_acks, 0},
        {max_batch_size, 131072},
        {max_linger_ms, 20},
        {compression, snappy},
        {max_retries, 3},
        {retry_backoff_ms, 100},
        {max_in_flight_requests, 10},
        {queue_buffering_max_messages, 100000},
        {queue_buffering_max_kbytes, 1048576}
     ]},

     {safe_opts, [
        {required_acks, -1},
        {enable_idempotence, true},
        {max_in_flight_requests, 5},
        {max_batch_size, 131072},
        {max_linger_ms, 20},
        {compression, snappy},
        {max_retries, 3},
        {retry_backoff_ms, 100}
     ]},

     {worker_count, 5},

     {headers_profiles,
        #{
           gateway_std =>
              #{
                 <<"device_type">> => <<"Gateway">>,
                 <<"model_status">> => <<"standard">>
              },
           sensor_raw =>
              #{
                 <<"device_type">> => <<"Sensor">>,
                 <<"model_status">> => <<"raw">>
              }
        }
     },

     {mappings, [
        #{
          pattern => "gateways/#",
          topic   => "sensor-data",
          headers_profile => gateway_std
        },
        #{
          pattern => "sensors/+",
          topic   => "sensor-data",
          headers_profile => sensor_raw
        }
     ]}
  ]}
].
```

### Key Sections

| Key               | Description |
|-------------------|-------------|
| `brokers`         | List of Kafka brokers `{Host, Port}`. |
| `client_opts`     | Common Kafka client options. |
| `fast_opts`       | Producer settings for low-latency, high-throughput publishing. |
| `safe_opts`       | Producer settings for reliable, idempotent publishing. |
| `worker_count`    | Number of concurrent worker processes. |
| `headers_profiles`| Map of reusable Kafka header profiles. |
| `mappings`        | MQTT → Kafka routing rules, each linked to a header profile. |

---

## 📂 Configuration File Location

Place `advanced.config` in VerneMQ’s configuration directory:

- **Package installations (Debian/Ubuntu, RPM):**
  ```
  /etc/vernemq/advanced.config
  ```
- **Standalone release / source build:**
  ```
  <vernemq_release_dir>/etc/advanced.config
  ```

---

## 🛠 Environment Preparation

**Minimum requirement:**  
`rebar 3.24.0+build.5444.refaf0a4253` on `Erlang/OTP 27` (`ERTS 15.2.7.1`)

### Linux (Debian/Ubuntu, Fedora, CentOS)

1. **Install Erlang/OTP 27**  
   Using [Erlang Solutions](https://www.erlang-solutions.com/downloads/):
   ```bash
   wget https://packages.erlang-solutions.com/erlang-solutions_2.0_all.deb
   sudo dpkg -i erlang-solutions_2.0_all.deb
   sudo apt-get update
   sudo apt-get install esl-erlang=1:27.*
   ```
   Verify:
   ```bash
   erl -version
   ```

2. **Install rebar3 ≥ 3.24.0**  
   ```bash
   wget https://github.com/erlang/rebar3/releases/download/3.24.0/rebar3
   chmod +x rebar3
   sudo mv rebar3 /usr/local/bin/
   ```
   Verify:
   ```bash
   rebar3 version
   ```

---

### Windows 10/11

1. **Install Erlang/OTP 27**  
   Download from [Erlang/OTP Downloads](https://www.erlang.org/downloads) and select version 27.x.  
   During installation, check **Add to PATH**.

   Verify in PowerShell:
   ```powershell
   erl -version
   ```

2. **Install rebar3 ≥ 3.24.0**  
   Download `rebar3.exe` from [GitHub Releases](https://github.com/erlang/rebar3/releases/tag/3.24.0)  
   Place it in a folder included in your `PATH` (e.g., `C:\Tools\rebar3`).

   Verify:
   ```powershell
   rebar3 version
   ```

---

## 🏗 Build Instructions

### Development build
```bash
rebar3 clean --all
rebar3 compile
```

### Production build (optimized, no debug info)
```bash
rebar3 clean --all
rebar3 as prod compile
rebar3 as prod release
```

---

## 📦 Installation

1. **Build the plugin** (see above).
2. **Copy plugin and dependencies** to VerneMQ’s plugin directory:
   ```bash
   sudo cp -r _build/prod/rel/vmq_kafka_plugin/lib/* /usr/lib/vernemq/plugins/
   ```
3. **Copy `advanced.config`** to VerneMQ’s config directory (see [Configuration File Location](#-configuration-file-location)).
4. **Enable the plugin**:
   ```bash
   sudo vmq-admin plugin enable --name vmq_kafka_plugin --path /usr/lib/vernemq/plugins
   ```
5. **Restart or reload** the plugin to apply changes.

---

## Licence

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

## Author

- [Marcello Travaglini](https://github.com/marcello-travaglini)