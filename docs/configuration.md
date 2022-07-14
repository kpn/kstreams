Settings are read using [pkgsettings](https://github.com/kpn-digital/py-pkgsettings).

## Settings

| Setting | Description | Default |
|---|-----|----|
| SERVICE_KSTREAMS_KAFKA_CONFIG_BOOTSTRAP_SERVERS | Kafka servers | `["localhost:9092"]` |
| SERVICE_KSTREAMS_KAFKA_CONFIG_SECURITY_PROTOCOL | Kafka security protocol | `PLAINTEXT` |
| SERVICE_KSTREAMS_KAFKA_TOPIC_PREFIX | Topic prefix | `dev-kpn-des--` |
| SERVICE_KSTREAMS_KAFKA_SSL_CERT_DATA | client certificate data | `None` |
| SERVICE_KSTREAMS_KAFKA_SSL_KEY_DATA | client private key data | `None` |
| SERVICE_KSTREAMS_KAFKA_SSL_CABUNDLE_DATA | cabundle data (not needed for cluster environments) | `None` |
| SERVICE_KSTREAMS_KAFKA_SSL_CONTEXT | `ssl_context` | `None` |

NB: private key data and certificates should NOT be stored in git. Usually the data is retreived from a
secret store like Vault.

## How to use with pkgsettings

```python
from pkgsettings import Settings
from kstreams.conf import settings as kstreams_settings
 

kstreams_settings.configure(
    SERVICE_KSTREAMS_KAFKA_CONFIG_BOOTSTRAP_SERVERS=["localhost:9092"],
    SERVICE_KSTREAMS_KAFKA_CONFIG_SECURITY_PROTOCOL="PLAINTEXT",
    SERVICE_KSTREAMS_KAFKA_TOPIC_PREFIX="dev-kpn-des--",
)
```
