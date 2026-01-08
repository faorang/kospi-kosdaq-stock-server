"""
KOSPI/KOSDAQ Stock Data MCP Server v2

데이터 소스:
- KRX Data Marketplace (data.krx.co.kr): 모든 데이터 (KRX 직접 또는 카카오 로그인)
- pykrx: 폴백용 (선택사항)

환경변수:
    KRX_LOGIN_METHOD: 로그인 방식 (krx 또는 kakao, 기본값: krx)
    KRX_ID, KRX_PW: KRX 직접 로그인용
    KAKAO_ID, KAKAO_PW: 카카오 로그인용
"""

import json
import logging
import os
from datetime import datetime
from typing import Dict, Any, Union, Optional

from mcp.server.fastmcp import FastMCP

# Configure logging to file (STDIO 모드에서 stderr 출력 방지)
LOG_FILE = os.path.expanduser("~/.kospi_kosdaq_mcp.log")
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
    ]
)
logger = logging.getLogger(__name__)
logger.info(f"MCP Server 시작, 로그 파일: {LOG_FILE}")

# Create MCP server
mcp = FastMCP(
    "kospi-kosdaq-stock-server",
    dependencies=["requests", "pandas", "playwright"]
)

# Global clients (lazy initialization)
_krx_client = None
_krx_client_initialized = False
_use_pykrx_fallback = False  # pykrx 폴백 비활성화 (KRX에서 로그인 필수화됨)

# Global variable to store ticker information in memory
TICKER_MAP: Dict[str, str] = {}


def _get_auth_error_message() -> str:
    """현재 로그인 방식에 맞는 인증 에러 메시지 반환"""
    login_method = os.environ.get("KRX_LOGIN_METHOD", "krx").lower()
    if login_method == "krx":
        return "데이터 조회에 필요한 인증 정보(KRX_ID, KRX_PW) 환경변수가 설정되지 않았습니다."
    else:
        return "데이터 조회에 필요한 인증 정보(KAKAO_ID, KAKAO_PW) 환경변수가 설정되지 않았습니다."


def _get_krx_client():
    """KRX Data Client 가져오기 (lazy init)"""
    global _krx_client, _krx_client_initialized

    # 이미 초기화 시도했으면 결과 반환
    if _krx_client_initialized:
        return _krx_client

    _krx_client_initialized = True

    # KRX 로그인 환경변수 확인 (KRX 직접 로그인 또는 카카오 로그인)
    krx_id = os.environ.get("KRX_ID")
    krx_pw = os.environ.get("KRX_PW")
    kakao_id = os.environ.get("KAKAO_ID")
    kakao_pw = os.environ.get("KAKAO_PW")
    login_method = os.environ.get("KRX_LOGIN_METHOD", "krx").lower()

    # 로그인 방식에 따른 자격증명 확인
    if login_method == "krx":
        if not krx_id or not krx_pw:
            logger.warning("KRX_LOGIN_METHOD=krx이지만 KRX_ID, KRX_PW 환경변수가 설정되지 않았습니다.")
            return None
    else:  # kakao
        if not kakao_id or not kakao_pw:
            logger.warning("KRX_LOGIN_METHOD=kakao이지만 KAKAO_ID, KAKAO_PW 환경변수가 설정되지 않았습니다.")
            return None

    try:
        from krx_data_client import KRXDataClient
        _krx_client = KRXDataClient(
            krx_id=krx_id,
            krx_pw=krx_pw,
            kakao_id=kakao_id,
            kakao_pw=kakao_pw,
            login_method=login_method,
            headless=True,
            auto_login=True
        )
        logger.info("KRX Data Client 초기화 완료")
    except Exception as e:
        logger.error(f"KRX Data Client 초기화 실패: {e}")
        _krx_client = None

    return _krx_client


def _use_pykrx():
    """pykrx 사용 가능 여부 확인"""
    if not _use_pykrx_fallback:
        return False
    try:
        import pykrx
        return True
    except ImportError:
        return False


# =============================================================================
# Helper Functions
# =============================================================================

def validate_date(date_str: Union[str, int]) -> str:
    """날짜 형식 검증 및 변환"""
    try:
        if isinstance(date_str, int):
            date_str = str(date_str)
        if '-' in date_str:
            parsed_date = datetime.strptime(date_str, '%Y-%m-%d')
            return parsed_date.strftime('%Y%m%d')
        datetime.strptime(date_str, '%Y%m%d')
        return date_str
    except ValueError:
        raise ValueError(f"Date must be in YYYYMMDD format. Input value: {date_str}")


def validate_ticker(ticker_str: Union[str, int]) -> str:
    """티커 형식 검증"""
    if isinstance(ticker_str, int):
        return str(ticker_str).zfill(6)
    return ticker_str.zfill(6)


def df_to_dict_with_date_index(df, reverse: bool = True) -> Dict[str, Any]:
    """DataFrame을 날짜 키 딕셔너리로 변환"""
    if df is None or df.empty:
        return {}

    result = df.to_dict(orient='index')

    # datetime 인덱스를 문자열로 변환
    formatted = {}
    for k, v in result.items():
        if hasattr(k, 'strftime'):
            key = k.strftime('%Y-%m-%d')
        else:
            key = str(k)
        formatted[key] = v

    # 정렬
    sorted_items = sorted(formatted.items(), reverse=reverse)
    return dict(sorted_items)


# =============================================================================
# MCP Tools
# =============================================================================

@mcp.tool()
def load_all_tickers() -> Dict[str, str]:
    """Loads all ticker symbols and names for KOSPI and KOSDAQ into memory.

    Returns:
        Dict[str, str]: A dictionary mapping tickers to stock names.
        Example: {"005930": "삼성전자", "035720": "카카오", ...}
    """
    global TICKER_MAP

    # 캐시된 데이터가 있으면 반환
    if TICKER_MAP:
        logger.debug(f"Returning cached ticker information with {len(TICKER_MAP)} stocks")
        return TICKER_MAP

    try:
        # 1. KRX Data Client 시도
        client = _get_krx_client()
        if client:
            try:
                ticker_map = client.get_market_ticker_name(market="ALL")
                if ticker_map:
                    TICKER_MAP.update(ticker_map)
                    logger.info(f"KRX Data Client로 {len(TICKER_MAP)}개 종목 로드")
                    return TICKER_MAP
            except Exception as e:
                logger.warning(f"KRX Data Client 실패: {e}")

        # 2. pykrx 폴백 (비활성화됨 - KRX에서 로그인 필수화)
        if _use_pykrx():
            logger.info("pykrx로 폴백합니다...")
            from pykrx.stock.stock_api import get_nearest_business_day_in_a_week
            from pykrx.website.krx.market.wrap import get_market_ticker_and_name

            today = get_nearest_business_day_in_a_week()
            kospi_series = get_market_ticker_and_name(today, market="KOSPI")
            kosdaq_series = get_market_ticker_and_name(today, market="KOSDAQ")

            TICKER_MAP.update(kospi_series.to_dict())
            TICKER_MAP.update(kosdaq_series.to_dict())

            logger.info(f"pykrx로 {len(TICKER_MAP)}개 종목 로드")
            return TICKER_MAP

        return {"error": f"종목 정보 조회 실패. {_get_auth_error_message()}"}

    except Exception as e:
        error_message = f"Failed to retrieve ticker information: {str(e)}"
        logger.error(error_message)
        return {"error": error_message}


@mcp.resource("stock://tickers")
def get_ticker_map() -> str:
    """Retrieves the stored ticker symbol-name mapping information."""
    try:
        if not TICKER_MAP:
            return json.dumps({
                "message": "No ticker information stored. "
                           "Please run the load_all_tickers() tool first."
            })
        return json.dumps(TICKER_MAP)
    except Exception as e:
        return json.dumps({"error": f"Failed to retrieve ticker information: {str(e)}"})


@mcp.tool()
def get_stock_ohlcv(
    fromdate: Union[str, int],
    todate: Union[str, int],
    ticker: Union[str, int],
    adjusted: bool = True
) -> Dict[str, Any]:
    """Retrieves OHLCV (Open/High/Low/Close/Volume) data for a specific stock.

    Args:
        fromdate (str): Start date for retrieval (YYYYMMDD)
        todate   (str): End date for retrieval (YYYYMMDD)
        ticker   (str): Stock ticker symbol
        adjusted (bool, optional): Whether to use adjusted prices. Defaults to True.

    Returns:
        Dict with date keys containing OHLCV data.
        Columns: Open, High, Low, Close, Volume, Amount, MarketCap
    """
    try:
        fromdate = validate_date(fromdate)
        todate = validate_date(todate)
        ticker = validate_ticker(ticker)

        logger.debug(f"Retrieving stock OHLCV: {ticker}, {fromdate}-{todate}")

        # 1. KRX Data Client 시도
        client = _get_krx_client()
        if client:
            try:
                df = client.get_market_ohlcv(fromdate, todate, ticker, adjusted=adjusted)
                if not df.empty:
                    logger.info("KRX Data Client로 OHLCV 데이터 조회 성공")
                    return df_to_dict_with_date_index(df)
            except Exception as e:
                logger.warning(f"KRX Data Client 실패: {e}")

        # 2. pykrx 폴백 (비활성화됨 - KRX에서 로그인 필수화)
        if _use_pykrx():
            logger.info("pykrx로 폴백합니다...")
            from pykrx.stock.stock_api import get_market_ohlcv
            df = get_market_ohlcv(fromdate, todate, ticker, adjusted=adjusted)
            return df_to_dict_with_date_index(df)

        return {"error": f"OHLCV 데이터 조회 실패. {_get_auth_error_message()}"}

    except Exception as e:
        error_message = f"Data retrieval failed: {str(e)}"
        logger.error(error_message)
        return {"error": error_message}


@mcp.tool()
def get_stock_market_cap(
    fromdate: Union[str, int],
    todate: Union[str, int],
    ticker: Union[str, int]
) -> Dict[str, Any]:
    """Retrieves market capitalization data for a specific stock.

    Args:
        fromdate (str): Start date for retrieval (YYYYMMDD)
        todate   (str): End date for retrieval (YYYYMMDD)
        ticker   (str): Stock ticker symbol

    Returns:
        Dict with date keys containing MarketCap, Volume, Amount.
    """
    try:
        fromdate = validate_date(fromdate)
        todate = validate_date(todate)
        ticker = validate_ticker(ticker)

        logger.debug(f"Retrieving market cap: {ticker}, {fromdate}-{todate}")

        # 1. KRX Data Client 시도
        client = _get_krx_client()
        if client:
            try:
                df = client.get_market_cap(fromdate, todate, ticker)
                if not df.empty:
                    logger.info("KRX Data Client로 시가총액 데이터 조회 성공")
                    return df_to_dict_with_date_index(df)
            except Exception as e:
                logger.warning(f"KRX Data Client 실패: {e}")

        # 2. pykrx 폴백 (비활성화됨 - KRX에서 로그인 필수화)
        if _use_pykrx():
            logger.info("pykrx로 폴백합니다...")
            from pykrx.stock.stock_api import get_market_cap
            df = get_market_cap(fromdate, todate, ticker)
            return df_to_dict_with_date_index(df)

        return {"error": f"시가총액 데이터 조회 실패. {_get_auth_error_message()}"}

    except Exception as e:
        error_message = f"Data retrieval failed: {str(e)}"
        logger.error(error_message)
        return {"error": error_message}


@mcp.tool()
def get_stock_fundamental(
    fromdate: Union[str, int],
    todate: Union[str, int],
    ticker: Union[str, int]
) -> Dict[str, Any]:
    """Retrieves fundamental data (PER/PBR/Dividend Yield) for a specific stock.

    Args:
        fromdate (str): Start date for retrieval (YYYYMMDD)
        todate   (str): End date for retrieval (YYYYMMDD)
        ticker   (str): Stock ticker symbol

    Returns:
        Dict with date keys containing BPS, PER, PBR, EPS, DIV, DPS.
    """
    try:
        fromdate = validate_date(fromdate)
        todate = validate_date(todate)
        ticker = validate_ticker(ticker)

        logger.debug(f"Retrieving fundamental: {ticker}, {fromdate}-{todate}")

        # 1. KRX Data Client 시도 (기간 조회 지원)
        client = _get_krx_client()
        if client:
            try:
                df = client.get_market_fundamental(fromdate, todate, ticker)
                if not df.empty:
                    logger.info("KRX Data Client로 fundamental 데이터 조회 성공")
                    return df_to_dict_with_date_index(df)
            except Exception as e:
                logger.warning(f"KRX Data Client 실패: {e}")

        # 2. pykrx 폴백 (비활성화됨 - KRX에서 로그인 필수화)
        if _use_pykrx():
            logger.info("pykrx로 폴백합니다...")
            from pykrx.stock.stock_api import get_market_fundamental_by_date
            df = get_market_fundamental_by_date(fromdate, todate, ticker)
            return df_to_dict_with_date_index(df)

        return {"error": f"Fundamental 데이터 조회 실패. {_get_auth_error_message()}"}

    except Exception as e:
        error_message = f"Data retrieval failed: {str(e)}"
        logger.error(error_message)
        return {"error": error_message}


@mcp.tool()
def get_stock_trading_volume(
    fromdate: Union[str, int],
    todate: Union[str, int],
    ticker: Union[str, int],
    detail: bool = False
) -> Dict[str, Any]:
    """Retrieves trading volume by investor type for a specific stock.

    Args:
        fromdate (str): Start date for retrieval (YYYYMMDD)
        todate   (str): End date for retrieval (YYYYMMDD)
        ticker   (str): Stock ticker symbol
        detail   (bool): If True, returns 12 investor types. If False, returns 5 types.
                        False: 기관합계, 기타법인, 개인, 외국인합계, 전체
                        True: 금융투자, 보험, 투신, 사모, 은행, 기타금융, 연기금, 기타법인, 개인, 외국인, 기타외국인, 전체

    Returns:
        Dict with date keys containing investor type net trading volumes.
    """
    try:
        fromdate = validate_date(fromdate)
        todate = validate_date(todate)
        ticker = validate_ticker(ticker)

        logger.debug(f"Retrieving trading volume: {ticker}, {fromdate}-{todate}, detail={detail}")

        # 1. KRX Data Client 시도
        client = _get_krx_client()
        if client:
            try:
                df = client.get_market_trading_volume_by_date(fromdate, todate, ticker, detail=detail)
                if not df.empty:
                    logger.info("KRX Data Client로 투자자별 거래량 조회 성공")
                    return df_to_dict_with_date_index(df)
            except Exception as e:
                logger.warning(f"KRX Data Client 실패: {e}")

        # 2. pykrx 폴백 (비활성화됨 - KRX에서 로그인 필수화)
        if _use_pykrx():
            logger.info("pykrx로 폴백합니다...")
            from pykrx.stock.stock_api import get_market_trading_volume_by_date
            df = get_market_trading_volume_by_date(fromdate, todate, ticker)
            return df_to_dict_with_date_index(df)

        return {"error": f"투자자별 거래량 조회 실패. {_get_auth_error_message()}"}

    except Exception as e:
        error_message = f"Data retrieval failed: {str(e)}"
        logger.error(error_message)
        return {"error": error_message}


@mcp.tool()
def get_index_ohlcv(
    fromdate: Union[str, int],
    todate: Union[str, int],
    ticker: Union[str, int],
    freq: str = 'd'
) -> Dict[str, Any]:
    """Retrieves OHLCV data for a specific index.

    Args:
        fromdate (str): Start date for retrieval (YYYYMMDD)
        todate   (str): End date for retrieval (YYYYMMDD)
        ticker   (str): Index ticker (1001 for KOSPI, 2001 for KOSDAQ)
        freq     (str): d - daily / m - monthly / y - yearly. Defaults to 'd'.

    Returns:
        Dict with date keys containing index OHLCV data.
        Columns: Open, High, Low, Close, Volume, Amount
    """
    def validate_freq(freq_str: str) -> str:
        valid_freqs = ['d', 'm', 'y']
        if freq_str not in valid_freqs:
            raise ValueError(f"Frequency must be one of {valid_freqs}.")
        return freq_str

    try:
        fromdate = validate_date(fromdate)
        todate = validate_date(todate)
        ticker = str(ticker)  # 지수는 문자열로
        freq = validate_freq(freq)

        logger.debug(f"Retrieving index OHLCV: {ticker}, {fromdate}-{todate}, freq={freq}")

        # 1. KRX Data Client 시도
        client = _get_krx_client()
        if client:
            try:
                df = client.get_index_ohlcv(fromdate, todate, ticker, freq=freq)
                if not df.empty:
                    logger.info("KRX Data Client로 지수 OHLCV 조회 성공")
                    return df_to_dict_with_date_index(df)
            except Exception as e:
                logger.warning(f"KRX Data Client 실패: {e}")

        # 2. pykrx 폴백 (비활성화됨 - KRX에서 로그인 필수화)
        if _use_pykrx():
            logger.info("pykrx로 폴백합니다...")
            from pykrx.stock.stock_api import get_index_ohlcv_by_date
            df = get_index_ohlcv_by_date(fromdate, todate, ticker, freq=freq, name_display=False)
            return df_to_dict_with_date_index(df)

        return {"error": f"지수 데이터 조회 실패. {_get_auth_error_message()}"}

    except Exception as e:
        error_message = f"Data retrieval failed: {str(e)}"
        logger.error(error_message)
        return {"error": error_message}


# =============================================================================
# Resources
# =============================================================================

@mcp.resource("stock://format-guide")
def get_format_guide() -> str:
    """Provides a guide for date format and ticker symbol input."""
    return """
    [Input Format Guide]
    1. Ticker symbol: 6-digit number (e.g., 005930 - Samsung Electronics)
    2. Date format: YYYYMMDD (e.g., 20240301) or YYYY-MM-DD (e.g., 2024-03-01)

    [Notes]
    - The start date must be earlier than the end date.
    - For adjusted=True, adjusted prices are retrieved.
    """


@mcp.resource("stock://popular-tickers")
def get_popular_tickers() -> str:
    """Provides a list of frequently queried ticker symbols."""
    return """
    [Frequently Queried Ticker Symbols]
    - 005930: 삼성전자
    - 000660: SK하이닉스
    - 373220: LG에너지솔루션
    - 035420: NAVER
    - 035720: 카카오
    """


@mcp.resource("stock://data-sources")
def get_data_sources() -> str:
    """현재 사용 중인 데이터 소스 정보"""
    sources = []

    # KRX Data Client
    client = _get_krx_client()
    if client:
        sources.append("- KRX Data Client: 활성화 (OHLCV, 시가총액, PER/PBR, 투자자별 거래량, 지수)")
    else:
        sources.append(f"- KRX Data Client: 비활성화 ({_get_auth_error_message()})")

    # pykrx
    if _use_pykrx():
        sources.append("- pykrx: 폴백으로 사용 가능")
    else:
        sources.append("- pykrx: 비활성화")

    return "\n".join(["[Data Sources]"] + sources)


@mcp.resource("stock://index-tickers")
def get_index_tickers() -> str:
    """지수 티커 목록"""
    return """
    [Index Tickers]
    KOSPI 계열:
    - 1001: 코스피
    - 1028: KOSPI 200
    - 1034: KOSPI 100
    - 1035: KOSPI 50

    KOSDAQ 계열:
    - 2001: 코스닥
    - 2203: KOSDAQ 150
    """


# =============================================================================
# Prompts
# =============================================================================

@mcp.prompt()
def search_stock_data_prompt() -> str:
    """Prompt template for searching stock data."""
    return """
    Step-by-step guide for searching stock data by stock name:

    1. First, load the ticker information for all stocks:
       load_all_tickers()

    2. Check the code of the desired stock from the loaded ticker information:
       Refer to the stock://tickers resource.

    3. Retrieve the desired data using the found ticker:

       Retrieve OHLCV (Open/High/Low/Close/Volume) data:
       get_stock_ohlcv("start_date", "end_date", "ticker", adjusted=True)

       Retrieve market capitalization data:
       get_stock_market_cap("start_date", "end_date", "ticker")

       Retrieve fundamental indicators (PER/PBR/Dividend Yield):
       get_stock_fundamental("start_date", "end_date", "ticker")

       Retrieve trading volume by investor type:
       get_stock_trading_volume("start_date", "end_date", "ticker", detail=False)
       - detail=False: 5 investor types (기관합계, 기타법인, 개인, 외국인합계, 전체)
       - detail=True: 12 investor types (금융투자, 보험, 투신, 사모, 은행, 기타금융, 연기금, 기타법인, 개인, 외국인, 기타외국인, 전체)

       Retrieve index OHLCV data (KOSPI, KOSDAQ, etc.):
       get_index_ohlcv("start_date", "end_date", "ticker", freq="d")
       - ticker: 1001 for KOSPI, 2001 for KOSDAQ
       - freq: "d" for daily, "m" for monthly, "y" for yearly

    Example) To retrieve data for Samsung Electronics in December 2025:
    1. load_all_tickers()
    2. Refer to stock://tickers  # Samsung = 005930
    3. get_stock_ohlcv("20251201", "20251220", "005930")
    """


@mcp.prompt()
def get_stock_data_prompt() -> str:
    """Prompt template for retrieving stock data."""
    return """
    Please enter the following information to retrieve stock OHLCV data:

    1. Ticker symbol: 6-digit number (e.g., 005930)
    2. Start date: YYYYMMDD format (e.g., 20240101)
    3. End date: YYYYMMDD format (e.g., 20240301)
    4. Adjusted price: True/False (default: True)

    Example) get_stock_ohlcv("20240101", "20240301", "005930", adjusted=True)
    """


def main():
    mcp.run()


if __name__ == "__main__":
    main()
