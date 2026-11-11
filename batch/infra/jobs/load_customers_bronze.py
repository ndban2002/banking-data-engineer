"""Load PostgreSQL customers observations into Iceberg Bronze."""

from bronze_loader import main


if __name__ == "__main__":
    main("customers")
