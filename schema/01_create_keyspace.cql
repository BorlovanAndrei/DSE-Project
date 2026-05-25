CREATE KEYSPACE IF NOT EXISTS hotel
  WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 2
  };