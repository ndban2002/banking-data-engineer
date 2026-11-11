"""Load PostgreSQL accounts observations into Iceberg Bronze."""

from bronze_loader import main


if __name__ == "__main__":
    main("accounts")
