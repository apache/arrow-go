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

crypto_factory = pe.CryptoFactory(lambda config: MockKmsClient(config))
kms_connection_config = pe.KmsConnectionConfig()
decryption_config = pe.DecryptionConfiguration()
decryption_properties = crypto_factory.file_decryption_properties(kms_connection_config, decryption_config)

# Read back stats
# input_file = "pyarrow_encrypted_uniform.parquet"
input_file = "arrowgo_encrypted_uniform.parquet"
print(f"\nReading {input_file}")
with pq.ParquetFile(
        input_file,
        decryption_properties=decryption_properties) as f:
    # meta data
    rg = f.metadata.row_group(0)
    # per-column stats
    for col_idx in range(f.metadata.num_columns):
        col = rg.column(col_idx)
        statistics = "None" if col.statistics is None else str(col.statistics)
        print(f"Column '{col.path_in_schema}' statistics:\n  {statistics}")
    
    # read the table
    table = f.read()
    print("\nDecrypted table:")
    print(table)

print(f"\nSuccessfully read and decrypted {input_file}")    