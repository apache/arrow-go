import base64
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.parquet.encryption as pe

KEY_ID = "footer_key"

class MockKmsClient(pe.KmsClient):
    def __init__(self, kms_connection_configuration):
        super().__init__()

    def wrap_key(self, key_bytes, master_key_identifier):
        return base64.b64encode(key_bytes)

    def unwrap_key(self, wrapped_key, master_key_identifier):
        return base64.b64decode(wrapped_key)

# ── Write the file ───────────────────────────────────────────────────────
table = pa.table({"id": [1, 2, 3], "name": ["alice", "bob", "charlie"]})

crypto_factory = pe.CryptoFactory(lambda config: MockKmsClient(config))
kms_connection_config = pe.KmsConnectionConfig()
encryption_config = pe.EncryptionConfiguration(
        KEY_ID,
        uniform_encryption=True,
        plaintext_footer=False)

enc_props = crypto_factory.file_encryption_properties(
    kms_connection_config,
    encryption_config
)

output_file = "pyarrow_encrypted_uniform.parquet"
pq.write_table(table, output_file, compression="none", encryption_properties=enc_props)

print(f"Successfully written {output_file}")
print(f"   Key ID: {KEY_ID}")
print("   → This file MUST be readable by any correct arrow-go implementation")