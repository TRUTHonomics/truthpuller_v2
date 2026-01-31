"""
# INSTRUCTIONS:
# Dit script genereert crypto_assets.toml uit de symbols.symbols database tabel.
# Het haalt alle actieve crypto assets op en maakt een TOML bestand met 
# ticker variaties voor asset detectie in posts.
#
# Vereist: SSH tunnel naar database (ssh -L 15432:localhost:5432 bart@10.10.10.1)
#
# Draai vanuit de truthpuller_v2 directory met actieve venv:
#   python scripts/generate_asset_toml.py
#   python scripts/generate_asset_toml.py --output config/assets/crypto_assets.toml
#
# Output: config/assets/crypto_assets.toml
"""

import sys
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional
import yaml
import psycopg2
from psycopg2.extras import RealDictCursor

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Common crypto name mappings (base symbol -> full names/aliases)
CRYPTO_ALIASES = {
    'BTC': ['Bitcoin', 'BTC'],
    'ETH': ['Ethereum', 'ETH', 'Ether'],
    'SOL': ['Solana', 'SOL'],
    'ADA': ['Cardano', 'ADA'],
    'XRP': ['Ripple', 'XRP'],
    'DOGE': ['Dogecoin', 'DOGE', 'Doge'],
    'DOT': ['Polkadot', 'DOT'],
    'AVAX': ['Avalanche', 'AVAX'],
    'SHIB': ['Shiba Inu', 'SHIB', 'Shiba'],
    'MATIC': ['Polygon', 'MATIC'],
    'LTC': ['Litecoin', 'LTC'],
    'LINK': ['Chainlink', 'LINK'],
    'UNI': ['Uniswap', 'UNI'],
    'ATOM': ['Cosmos', 'ATOM'],
    'XLM': ['Stellar', 'XLM'],
    'ALGO': ['Algorand', 'ALGO'],
    'VET': ['VeChain', 'VET'],
    'FIL': ['Filecoin', 'FIL'],
    'NEAR': ['NEAR Protocol', 'NEAR'],
    'APT': ['Aptos', 'APT'],
    'ARB': ['Arbitrum', 'ARB'],
    'OP': ['Optimism', 'OP'],
    'SUI': ['Sui', 'SUI'],
    'PEPE': ['Pepe', 'PEPE'],
    'WIF': ['dogwifhat', 'WIF'],
    'BONK': ['Bonk', 'BONK'],
    'TRUMP': ['Trump', 'TRUMP', 'Official Trump'],
    'MELANIA': ['Melania', 'MELANIA'],
    'BNB': ['Binance Coin', 'BNB', 'Binance'],
    'TRX': ['Tron', 'TRX'],
    'TON': ['Toncoin', 'TON'],
    'ICP': ['Internet Computer', 'ICP'],
    'INJ': ['Injective', 'INJ'],
    'RENDER': ['Render', 'RENDER', 'RNDR'],
    'FET': ['Fetch.ai', 'FET'],
    'AAVE': ['Aave', 'AAVE'],
    'MKR': ['Maker', 'MKR'],
    'CRV': ['Curve', 'CRV'],
    'LDO': ['Lido', 'LDO'],
    'SAND': ['Sandbox', 'SAND'],
    'MANA': ['Decentraland', 'MANA'],
    'AXS': ['Axie Infinity', 'AXS'],
    'GALA': ['Gala', 'GALA'],
    'IMX': ['Immutable', 'IMX'],
    'ENS': ['Ethereum Name Service', 'ENS'],
    'APE': ['ApeCoin', 'APE'],
    'GMT': ['STEPN', 'GMT'],
    'BLUR': ['Blur', 'BLUR'],
    'SEI': ['Sei', 'SEI'],
    'TIA': ['Celestia', 'TIA'],
    'JUP': ['Jupiter', 'JUP'],
    'STRK': ['Starknet', 'STRK'],
    'W': ['Wormhole', 'W'],
    'ENA': ['Ethena', 'ENA'],
    'HBAR': ['Hedera', 'HBAR'],
    'FTM': ['Fantom', 'FTM'],
    'RUNE': ['THORChain', 'RUNE'],
    'XMR': ['Monero', 'XMR'],
    'ETC': ['Ethereum Classic', 'ETC'],
    'BCH': ['Bitcoin Cash', 'BCH'],
    'BSV': ['Bitcoin SV', 'BSV'],
    'ZEC': ['Zcash', 'ZEC'],
    'DASH': ['Dash', 'DASH'],
    'EOS': ['EOS', 'EOS'],
    'NEO': ['NEO', 'NEO'],
    'XTZ': ['Tezos', 'XTZ'],
    'KAVA': ['Kava', 'KAVA'],
    'KSM': ['Kusama', 'KSM'],
    'FLOW': ['Flow', 'FLOW'],
    'EGLD': ['MultiversX', 'EGLD'],
    'QNT': ['Quant', 'QNT'],
    'THETA': ['Theta', 'THETA'],
    'GRT': ['The Graph', 'GRT'],
    'SNX': ['Synthetix', 'SNX'],
    'COMP': ['Compound', 'COMP'],
    'YFI': ['Yearn', 'YFI'],
    'SUSHI': ['SushiSwap', 'SUSHI'],
    '1INCH': ['1inch', '1INCH'],
    'BAL': ['Balancer', 'BAL'],
    'ZRX': ['0x', 'ZRX'],
    'ENJ': ['Enjin', 'ENJ'],
    'CHZ': ['Chiliz', 'CHZ'],
    'MASK': ['Mask Network', 'MASK'],
    'DYDX': ['dYdX', 'DYDX'],
    'GMX': ['GMX', 'GMX'],
    'MAGIC': ['Magic', 'MAGIC'],
    'PENDLE': ['Pendle', 'PENDLE'],
    'SSV': ['SSV Network', 'SSV'],
    'RPL': ['Rocket Pool', 'RPL'],
    'FXS': ['Frax Share', 'FXS'],
    'CVX': ['Convex', 'CVX'],
    'ONDO': ['Ondo', 'ONDO'],
    'PYTH': ['Pyth', 'PYTH'],
    'JTO': ['Jito', 'JTO'],
    'MEME': ['Memecoin', 'MEME'],
    'WLD': ['Worldcoin', 'WLD'],
    'AI': ['AI', 'AI'],
}


def load_config() -> Dict:
    """Load configuration from truth_config.yaml."""
    config_path = Path(__file__).parent.parent / 'config' / 'truth_config.yaml'
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def get_db_connection(config: Dict):
    """Get database connection using config."""
    db_config = {
        'host': config['database']['host'],
        'port': config['database']['port'],
        'dbname': config['database']['dbname'],
        'user': config['database']['user'],
        'password': config['database']['password']
    }
    return psycopg2.connect(**db_config)


def fetch_crypto_assets(conn) -> List[Dict]:
    """
    Fetch all active crypto assets from symbols.symbols.
    
    Returns:
        List of asset dicts with id, bybit_symbol, base, quote
    """
    query = """
        SELECT 
            id,
            bybit_symbol,
            kraken_symbol,
            base,
            quote,
            type
        FROM symbols.symbols
        WHERE tradeable = true
        ORDER BY id
    """
    
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(query)
    assets = [dict(row) for row in cursor.fetchall()]
    cursor.close()
    
    logger.info(f"Fetched {len(assets)} assets from database")
    return assets


def generate_tickers(asset: Dict) -> List[str]:
    """
    Generate list of ticker variations for an asset.
    
    Args:
        asset: Asset dict with base, bybit_symbol, kraken_symbol
        
    Returns:
        List of unique ticker strings (case-preserved for TOML)
    """
    tickers = set()
    base = asset.get('base', '').upper()
    bybit = asset.get('bybit_symbol', '')
    kraken = asset.get('kraken_symbol', '')
    
    # Add base symbol
    if base:
        tickers.add(base)
    
    # Add full symbols
    if bybit:
        tickers.add(bybit.upper())
    if kraken:
        tickers.add(kraken.upper())
    
    # Add known aliases
    if base in CRYPTO_ALIASES:
        for alias in CRYPTO_ALIASES[base]:
            tickers.add(alias.upper())
            # Also add original case for names
            tickers.add(alias)
    
    return sorted(list(tickers))


def generate_toml_content(assets: List[Dict]) -> str:
    """
    Generate TOML file content from asset list.
    
    Args:
        assets: List of asset dicts
        
    Returns:
        TOML formatted string
    """
    lines = [
        "# Crypto Assets Configuration",
        "# Generated from symbols.symbols database table",
        f"# Generated at: {datetime.now(timezone.utc).isoformat()}",
        "#",
        "# Structure per asset:",
        "# [assets.<SYMBOL>]",
        "# asset_id = <database id>",
        "# symbol = \"<bybit_symbol>\"",
        "# base = \"<base currency>\"",
        "# quote = \"<quote currency>\"",
        "# tickers = [\"TICKER1\", \"TICKER2\", ...]  # variations for text matching",
        "",
        "[meta]",
        "asset_type = \"crypto\"",
        f"generated_at = \"{datetime.now(timezone.utc).isoformat()}\"",
        f"total_assets = {len(assets)}",
        "",
        "[assets]",
        ""
    ]
    
    for asset in assets:
        asset_id = asset['id']
        bybit_symbol = asset.get('bybit_symbol', '')
        base = asset.get('base', '')
        quote = asset.get('quote', 'USDT')
        
        # Use bybit_symbol as the key (without special chars)
        key = bybit_symbol.replace('.', '_').replace('-', '_') if bybit_symbol else f"ASSET_{asset_id}"
        
        tickers = generate_tickers(asset)
        tickers_str = ', '.join([f'"{t}"' for t in tickers])
        
        lines.append(f"[assets.{key}]")
        lines.append(f"asset_id = {asset_id}")
        lines.append(f"symbol = \"{bybit_symbol}\"")
        lines.append(f"base = \"{base}\"")
        lines.append(f"quote = \"{quote}\"")
        lines.append(f"tickers = [{tickers_str}]")
        lines.append("")
    
    return '\n'.join(lines)


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate crypto_assets.toml from database')
    parser.add_argument(
        '--output', '-o',
        type=str,
        default=None,
        help='Output file path (default: config/assets/crypto_assets.toml)'
    )
    args = parser.parse_args()
    
    # Determine output path
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = Path(__file__).parent.parent / 'config' / 'assets' / 'crypto_assets.toml'
    
    # Ensure directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    logger.info("Loading configuration...")
    config = load_config()
    
    logger.info("Connecting to database...")
    conn = get_db_connection(config)
    
    try:
        logger.info("Fetching crypto assets...")
        assets = fetch_crypto_assets(conn)
        
        logger.info("Generating TOML content...")
        toml_content = generate_toml_content(assets)
        
        logger.info(f"Writing to {output_path}...")
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(toml_content)
        
        logger.info(f"Successfully generated {output_path}")
        logger.info(f"Total assets: {len(assets)}")
        
    finally:
        conn.close()


if __name__ == "__main__":
    main()

