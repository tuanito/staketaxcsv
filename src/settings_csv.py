import os

# Environment variables (required for each respective report)

ALGO_HIST_INDEXER_NODE = os.environ.get("ALGO_HIST_INDEXER_NODE", "https://indexer.algoexplorerapi.io")
ALGO_INDEXER_NODE = os.environ.get("ALGO_INDEXER_NODE", "https://algoindexer.algoexplorerapi.io")
ALGO_NFDOMAINS = os.environ.get("ALGO_NFDOMAINS", "https://api.nf.domains")
ATOM_NODE = os.environ.get("ATOM_NODE", "")
COVALENT_NODE = os.environ.get("COVALENT_NODE", "https://api.covalenthq.com")
DVPN_LCD_NODE = os.environ.get("DVPN_LCD_NODE", "https://lcd.sentinel.co")
DVPN_RPC_NODE = os.environ.get("DVPN_RPC_NODE", "https://rpc.sentinel.co")
FET_NODE = os.environ.get("FET_NODE", "https://rest-fetchhub.fetch.ai")

HUAHUA_NODE = os.environ.get("HUAHUA_NODE", "")
JUNO_NODE = os.environ.get("JUNO_NODE", "")
BTSG_NODE = os.environ.get("BTSG_NODE", "https://lcd.explorebitsong.com")
STARS_NODE = os.environ.get("STARS_NODE", "")
#SOL_NODE = os.environ.get("SOL_NODE", "http://tyo17.rpcpool.com/0e333ff99137bd8afe70ca703692")
#SOL_NODE = os.environ.get("SOL_NODE", "https://api.devnet.solana.com")
SOL_NODE = os.environ.get("SOL_NODE", "https://solana-api.projectserum.com")

TERRA_LCD_NODE = os.environ.get("TERRA_LCD_NODE", "")
LUNA2_LCD_NODE = os.environ.get("LUNA2_LCD_NODE", "https://phoenix-lcd.terra.dev")

# Optional environment variables
COVALENT_API_KEY = os.environ.get("COVALENT_API_KEY", "")

# #############################################################################

TICKER_ALGO = "ALGO"
TICKER_ATOM = "ATOM"
TICKER_DVPN = "DVPN"
TICKER_FET = "FET"
TICKER_HUAHUA = "HUAHUA"
TICKER_IOTEX = "IOTX"
TICKER_JUNO = "JUNO"
TICKER_BTSG = "BTSG"
TICKER_STARS = "STARS"
TICKER_LUNA1 = "LUNA1"
TICKER_LUNA2 = "LUNA2"
TICKER_OSMO = "OSMO"
TICKER_SOL = "SOL"

DONATION_WALLETS = set([
    os.environ.get("DONATION_WALLET_ALGO", ""),
    os.environ.get("DONATION_WALLET_ATOM", ""),
    os.environ.get("DONATION_WALLET_FET", ""),
    os.environ.get("DONATION_WALLET_HUAHUA", ""),
    os.environ.get("DONATION_WALLET_IOTX", ""),
    os.environ.get("DONATION_WALLET_JUNO", ""),
    os.environ.get("DONATION_WALLET_BTSG", ""),
    os.environ.get("DONATION_WALLET_STARS", ""),
    os.environ.get("DONATION_WALLET_LUNA", ""),
    os.environ.get("DONATION_WALLET_OSMO", ""),
    os.environ.get("DONATION_WALLET_SOL", ""),
])

MESSAGE_ADDRESS_NOT_FOUND = "Wallet address not found"
MESSAGE_STAKING_ADDRESS_FOUND = "Staking address found.  Please input the main wallet address instead."

REPORTS_DIR = os.path.dirname(os.path.realpath(__file__)) + "/_reports"
