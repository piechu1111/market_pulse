from ingestion_alpha_vantage import fetch_and_store_alpha_vantage_data

def handler(event, context):
    return fetch_and_store_alpha_vantage_data(
        symbols_path=event["symbols_s3_uri"],
        start_year_month=event["year_month_start"],
        end_year_month=event["year_month_end"],
        symbols_subset=event.get("symbols_subset"),
        limit=event.get("limit"),
    )