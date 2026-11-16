# Hardware & Performance Specification: Raspberry Pi 5 (8GB RAM / 500GB NVMe SSD)

## 1. System Architecture & Processing Core
* **System-on-Chip (SoC):** Broadcom BCM2712
* **CPU:** Quad-core 64-bit Arm Cortex-A76 @ 2.4 GHz
  * **Architecture:** Armv8-A ISA
  * **Extensions:** Cryptographic Extension
  * **Cache Topology:** 64 KB I/D cache per core, 512 KB L2 cache per core, 2 MB shared L3 cache
* **GPU:** Broadcom VideoCore VII @ 800 MHz
  * **Graphics API Support:** OpenGL ES 3.1, Vulkan 1.2 / 1.3
* **I/O Controller:** RP1 Southbridge ASIC (handling PCIe, USB, Ethernet, GPIO, and MIPI transceivers)

---

## 2. Memory Subsystem
* **Capacity:** 8 GB
* **Type:** LPDDR4X-4267 SDRAM
* **Bus Width:** 32-bit channel architecture
* **Peak Memory Bandwidth:** ~17 GB/s

---

## 3. Primary Storage Interface (500GB SSD Integration)
* **Bus Interface:** PCIe 2.0 x1 exposed via a 16-pin, 0.5mm pitch FPC connector
* **Storage Protocol:** NVMe via M.2 HAT / Adapter board
* **Drive Specifications:** 500 GB M.2 NVMe SSD (Form factor: 2230, 2242, or 2280 depending on HAT configuration)
* **Throughput Profile:**
  * **PCIe Gen 2.0 (Default):** Peak bandwidth up to ~500 MB/s (5.0 GT/s link rate)
  * **PCIe Gen 3.0 (Software Override):** Peak bandwidth up to ~800–900 MB/s (8.0 GT/s link rate; non-standard/uncertified setting)
* **Secondary Storage:** MicroSD card slot supporting high-speed SDR104 mode

---

## 4. Networking & Wireless Connectivity
* **Wired Ethernet:** 10/100/1000 Mbps Gigabit Ethernet (PoE+ supported via external HAT)
* **Wi-Fi:** Dual-band 802.11ac (2.4 GHz and 5.0 GHz)
* **Bluetooth:** Bluetooth 5.0 / Bluetooth Low Energy (BLE)

---

## 5. Media & Display Hardware
* **Display Output:** 2 × micro-HDMI ports (Dual 4Kp60 output with HDR support)
* **Hardware Decoder:** 4Kp60 HEVC / H.265 hardware video decoder
* **Camera / Display Interface:** 2 × 4-lane MIPI transceivers (1.5 Gbps per lane, dual-use for camera or display)
* **Image Signal Processor (ISP):** Integrated Raspberry Pi ISP (up to 1 Gigapixel/sec throughput)

---

## 6. Peripheral Interfaces & I/O
* **USB Ports:**
  * 2 × USB 3.0 Type-A ports (Dedicated 5 Gbps simultaneous bandwidth per port via RP1)
  * 2 × USB 2.0 Type-A ports
* **GPIO:** Standard 40-pin GPIO header (3.3V logic level)
  * Multiplexed hardware peripheral blocks via RP1: UART, SPI, I2C, PWM
* **Real-Time Clock (RTC):** Integrated RTC powered via external battery connector
* **Control:** Onboard physical power button

---

## 7. Electrical & Thermal Specifications
* **Power Input:** 5V / 5A (25W) DC via USB Type-C with USB Power Delivery (PD) profile
* **Cooling Requirements:** Active thermal management (Active Cooler with aluminum heatsink and PWM variable-speed fan) 