#!/usr/bin/env python3
"""
Daily Direct Deposit Data Update Script
This script queries Snowflake for direct deposit data across multiple date ranges
and updates the index.html file with fresh data.
"""

import os
import json
from datetime import datetime, timedelta
import snowflake.connector
from typing import Dict, List, Any

# Snowflake connection parameters from environment variables
SNOWFLAKE_CONFIG = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': 'MYBAMBU_PROD',
    'schema': 'BAMBU_MART_GALILEO'
}

def get_snowflake_connection():
    """Establish connection to Snowflake"""
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)

def query_summary_stats(conn, start_date: str, end_date: str) -> Dict[str, Any]:
    """Query summary statistics for a date range"""
    query = f"""
    SELECT
        COUNT(DISTINCT PRN) as unique_users,
        COUNT(*) as total_transactions,
        SUM(CAST(TRANSACTION_AMOUNT AS FLOAT)) as total_volume,
        AVG(CAST(TRANSACTION_AMOUNT AS FLOAT)) as avg_deposit
    FROM MYBAMBU_PROD.BAMBU_MART_GALILEO.MART_SRDF_POSTED_TRANSACTIONS
    WHERE TRANSACTION_CODE = 'PMOF'
        AND POST_DATE >= '{start_date}'
        AND POST_DATE <= '{end_date}'
    """
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchone()
    cursor.close()

    return {
        'uniqueUsers': int(result[0]),
        'totalTransactions': int(result[1]),
        'totalVolume': float(result[2]),
        'avgDeposit': float(result[3])
    }

def query_buckets(conn, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """Query transaction amount buckets for a date range"""
    query = f"""
    SELECT
        CASE
            WHEN CAST(TRANSACTION_AMOUNT AS FLOAT) >= 0 AND CAST(TRANSACTION_AMOUNT AS FLOAT) < 250 THEN '$1-$250'
            WHEN CAST(TRANSACTION_AMOUNT AS FLOAT) >= 250 AND CAST(TRANSACTION_AMOUNT AS FLOAT) < 500 THEN '$250-$500'
            WHEN CAST(TRANSACTION_AMOUNT AS FLOAT) >= 500 AND CAST(TRANSACTION_AMOUNT AS FLOAT) < 750 THEN '$500-$750'
            WHEN CAST(TRANSACTION_AMOUNT AS FLOAT) >= 750 AND CAST(TRANSACTION_AMOUNT AS FLOAT) < 1000 THEN '$750-$1,000'
            WHEN CAST(TRANSACTION_AMOUNT AS FLOAT) >= 1000 AND CAST(TRANSACTION_AMOUNT AS FLOAT) < 1500 THEN '$1,000-$1,500'
            WHEN CAST(TRANSACTION_AMOUNT AS FLOAT) >= 1500 AND CAST(TRANSACTION_AMOUNT AS FLOAT) < 2500 THEN '$1,500-$2,500'
            ELSE '$2,500+'
        END as bucket,
        COUNT(*) as count
    FROM MYBAMBU_PROD.BAMBU_MART_GALILEO.MART_SRDF_POSTED_TRANSACTIONS
    WHERE TRANSACTION_CODE = 'PMOF'
        AND POST_DATE >= '{start_date}'
        AND POST_DATE <= '{end_date}'
    GROUP BY bucket
    ORDER BY MIN(CAST(TRANSACTION_AMOUNT AS FLOAT))
    """
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()

    bucket_order = ['$1-$250', '$250-$500', '$500-$750', '$750-$1,000',
                    '$1,000-$1,500', '$1,500-$2,500', '$2,500+']
    buckets = [{'range': bucket, 'count': int(count)} for bucket, count in results]
    buckets.sort(key=lambda x: bucket_order.index(x['range']))

    return buckets

def query_top_by_frequency(conn, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """Query top 10 ACH institutions by transaction frequency"""
    query = f"""
    SELECT
        ACH_INSTITUTION_NAME as name,
        COUNT(*) as count,
        SUM(CAST(TRANSACTION_AMOUNT AS FLOAT)) as volume
    FROM MYBAMBU_PROD.BAMBU_MART_GALILEO.MART_SRDF_POSTED_TRANSACTIONS
    WHERE TRANSACTION_CODE = 'PMOF'
        AND POST_DATE >= '{start_date}'
        AND POST_DATE <= '{end_date}'
        AND ACH_INSTITUTION_NAME IS NOT NULL
    GROUP BY ACH_INSTITUTION_NAME
    ORDER BY count DESC
    LIMIT 10
    """
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()

    return [{'name': str(name).strip(), 'count': int(count), 'volume': float(volume)}
            for name, count, volume in results]

def query_top_by_volume(conn, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """Query top 10 ACH institutions by transaction volume"""
    query = f"""
    SELECT
        ACH_INSTITUTION_NAME as name,
        COUNT(*) as count,
        SUM(CAST(TRANSACTION_AMOUNT AS FLOAT)) as volume
    FROM MYBAMBU_PROD.BAMBU_MART_GALILEO.MART_SRDF_POSTED_TRANSACTIONS
    WHERE TRANSACTION_CODE = 'PMOF'
        AND POST_DATE >= '{start_date}'
        AND POST_DATE <= '{end_date}'
        AND ACH_INSTITUTION_NAME IS NOT NULL
    GROUP BY ACH_INSTITUTION_NAME
    ORDER BY volume DESC
    LIMIT 10
    """
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()

    return [{'name': str(name).strip(), 'count': int(count), 'volume': float(volume)}
            for name, count, volume in results]

def get_date_ranges() -> Dict[str, tuple]:
    """Calculate date ranges for all presets"""
    today = datetime.now()

    return {
        'last_30': ((today - timedelta(days=30)).strftime('%Y-%m-%d'),
                    today.strftime('%Y-%m-%d')),
        'last_60': ((today - timedelta(days=60)).strftime('%Y-%m-%d'),
                    today.strftime('%Y-%m-%d')),
        'last_90': ((today - timedelta(days=90)).strftime('%Y-%m-%d'),
                    today.strftime('%Y-%m-%d')),
        'august_2025': ('2025-08-01', '2025-08-31'),
        'september_2025': ('2025-09-01', '2025-09-30'),
        'october_2025': ('2025-10-01', '2025-10-31'),
        'november_2025': ('2025-11-01', '2025-11-30')
    }

def query_all_data() -> Dict[str, Any]:
    """Query all data for all date ranges"""
    conn = get_snowflake_connection()
    date_ranges = get_date_ranges()
    data_cache = {}

    for range_name, (start_date, end_date) in date_ranges.items():
        print(f"Querying {range_name}: {start_date} to {end_date}")
        cache_key = f"{start_date}_{end_date}"

        data_cache[cache_key] = {
            'summary': query_summary_stats(conn, start_date, end_date),
            'buckets': query_buckets(conn, start_date, end_date),
            'topByFrequency': query_top_by_frequency(conn, start_date, end_date),
            'topByVolume': query_top_by_volume(conn, start_date, end_date)
        }

    conn.close()
    return data_cache

def update_html_file(data_cache: Dict[str, Any]):
    """Update index.html with new data"""
    html_path = 'index.html'

    with open(html_path, 'r') as f:
        html_content = f.read()

    # Find the dataCache section and replace it
    start_marker = 'const dataCache = {'
    end_marker = '        };'

    start_idx = html_content.find(start_marker)
    end_idx = html_content.find(end_marker, start_idx) + len(end_marker)

    if start_idx == -1 or end_idx == -1:
        raise ValueError("Could not find dataCache section in HTML file")

    # Format the new data cache
    new_data_cache = f"const dataCache = {json.dumps(data_cache, indent=12)[:-1]}        }};"

    # Replace the old data cache with the new one
    new_html = html_content[:start_idx] + new_data_cache + html_content[end_idx:]

    with open(html_path, 'w') as f:
        f.write(new_html)

    print(f"✓ Updated {html_path} with fresh data")

def main():
    """Main execution function"""
    print("=" * 60)
    print("Direct Deposit Data Update Script")
    print(f"Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    try:
        # Query all data
        print("\n📊 Querying Snowflake for all date ranges...")
        data_cache = query_all_data()

        # Update HTML file
        print("\n📝 Updating index.html...")
        update_html_file(data_cache)

        print("\n✅ Data update completed successfully!")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        raise

if __name__ == '__main__':
    main()
