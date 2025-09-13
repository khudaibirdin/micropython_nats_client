# Description
To collect data from the MCU on Micropython, there are only libraries with support for the MQTT protocol. Undoubtedly, this protocol is well suited for IOT interaction.
At the moment, it is possible to collect data using EDA systems, such as NATS, which is lightweight and the most modern and fastest.
The library allows you to implement the NATS PUBLISHER client in Micropython.

# Usage

## Installation
Copy micropython_nats_client folder into mcu directory.

## Pub to NATS
```python
from nats.publisher import Publisher


NATS_HOST = "192.168.31.102" # nats server host
NATS_PORT = 4222  # nats server port

# implement ethernet connection via Wi-Fi/LAN

np = Publisher(
    host=NATS_HOST,
    port=NATS_PORT
)

np.send(
    topic="test",
    msg=f"Test message"
)
```
