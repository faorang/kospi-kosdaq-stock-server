"""
KRX Data Client - 통합 주식 데이터 클라이언트

pykrx와 동일한 인터페이스로 KRX Data Marketplace에서 데이터를 조회합니다.
카카오 로그인이 필요하며, 2차인증은 비활성화되어 있어야 합니다.

아키텍처:
- KakaoAuthManager: 카카오 로그인 관리, 세션 유지, 자동 재로그인
- KRXDataClient: 실제 데이터 조회 API

환경변수:
    KAKAO_ID: 카카오 아이디
    KAKAO_PW: 카카오 비밀번호

사용법:
    from krx_data_client import KRXDataClient

    client = KRXDataClient()

    # pykrx와 동일한 인터페이스
    df = client.get_market_ohlcv("20240101", "20240131", "005930")
    df = client.get_market_fundamental("20240101", "20240131", "005930")
    df = client.get_market_trading_volume("20240101", "20240131", "005930")
"""

import os
import json
import logging
import asyncio
import functools
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List, Callable, TypeVar
from dataclasses import dataclass

# MCP 서버 등 이미 이벤트 루프가 실행 중인 환경에서 중첩 실행 허용
import nest_asyncio
nest_asyncio.apply()

import requests
import pandas as pd
from pandas import DataFrame

logger = logging.getLogger(__name__)

# 타입 힌트용
T = TypeVar('T')


class KRXAuthError(Exception):
    """인증 관련 에러"""
    pass


class KRX2FARequiredError(KRXAuthError):
    """2차인증이 활성화되어 있음"""
    def __init__(self):
        super().__init__(
            "카카오 2차인증이 활성화되어 있습니다.\n"
            "2차인증을 비활성화하세요:\n"
            "  - 카카오톡 > 설정 > 카카오계정 > 2단계 인증 > 해제\n"
            "  - 또는 https://accounts.kakao.com > 계정보안 > 2단계 인증 > 해제"
        )


class KRXSessionExpiredError(KRXAuthError):
    """세션 만료"""
    pass


class KRXDataError(Exception):
    """데이터 조회 에러"""
    pass


@dataclass
class SessionInfo:
    """세션 정보"""
    cookies: Dict[str, str]
    last_login: datetime
    expires_at: Optional[datetime] = None


def retry_on_session_expired(max_retries: int = 3, delay: float = 1.0):
    """세션 만료 시 재시도하는 데코레이터"""
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs) -> T:
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(self, *args, **kwargs)
                except KRXSessionExpiredError as e:
                    last_exception = e
                    logger.warning(f"세션 만료 (시도 {attempt + 1}/{max_retries}), 재로그인...")
                    time.sleep(delay)
                    try:
                        self._auth_manager.login(force=True)
                    except Exception as login_error:
                        logger.error(f"재로그인 실패: {login_error}")
                        raise
                except Exception as e:
                    # 다른 에러는 재시도하지 않음
                    raise
            raise last_exception
        return wrapper
    return decorator


class KakaoAuthManager:
    """
    카카오 로그인 관리자

    - 카카오 로그인/로그아웃
    - 2차인증 상태 체크 및 에러 발생
    - 세션 쿠키 저장/로드
    - 세션 만료 체크 및 자동 갱신
    """

    COOKIE_PATH = Path.home() / ".krx_session.json"
    LEGACY_COOKIE_PATH = Path.home() / ".krx_cookies.json"  # 기존 쿠키 파일
    SESSION_TIMEOUT = timedelta(hours=4)  # 세션 타임아웃 (보수적 설정)

    def __init__(
        self,
        kakao_id: Optional[str] = None,
        kakao_pw: Optional[str] = None,
        headless: bool = True,
    ):
        self.kakao_id = kakao_id or os.environ.get("KAKAO_ID")
        self.kakao_pw = kakao_pw or os.environ.get("KAKAO_PW")
        self.headless = headless

        self._session: Optional[requests.Session] = None
        self._session_info: Optional[SessionInfo] = None
        self._browser = None
        self._playwright = None

        if not self.kakao_id or not self.kakao_pw:
            raise KRXAuthError(
                "카카오 로그인 정보가 필요합니다.\n"
                "KAKAO_ID, KAKAO_PW 환경변수를 설정하세요."
            )

    @property
    def is_logged_in(self) -> bool:
        """로그인 상태 확인"""
        if not self._session_info:
            return False

        # 세션 타임아웃 체크
        if datetime.now() - self._session_info.last_login > self.SESSION_TIMEOUT:
            logger.info("세션 타임아웃")
            return False

        return True

    @property
    def session(self) -> requests.Session:
        """requests 세션 반환"""
        if not self._session:
            self._session = requests.Session()
            self._session.headers.update({
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
                "Origin": "http://data.krx.co.kr",
                "Referer": "http://data.krx.co.kr/",
            })
        return self._session

    def _load_session(self) -> bool:
        """저장된 세션 로드"""
        # 새 형식 파일 시도
        if self.COOKIE_PATH.exists():
            try:
                data = json.loads(self.COOKIE_PATH.read_text())
                cookies = data.get("cookies", {})
                last_login_str = data.get("last_login")

                if cookies and last_login_str:
                    last_login = datetime.fromisoformat(last_login_str)

                    # 타임아웃 체크
                    if datetime.now() - last_login <= self.SESSION_TIMEOUT:
                        # 세션에 쿠키 적용
                        for name, value in cookies.items():
                            self.session.cookies.set(name, value)

                        self._session_info = SessionInfo(
                            cookies=cookies,
                            last_login=last_login
                        )

                        logger.info("저장된 세션을 로드했습니다.")
                        return True
                    else:
                        logger.info("저장된 세션이 만료되었습니다.")
            except Exception as e:
                logger.warning(f"세션 로드 실패: {e}")

        # 기존 형식 파일 (krx_crawler_client 호환)
        if self.LEGACY_COOKIE_PATH.exists():
            try:
                cookies_list = json.loads(self.LEGACY_COOKIE_PATH.read_text())
                if isinstance(cookies_list, list) and cookies_list:
                    cookies = {c["name"]: c["value"] for c in cookies_list}

                    # 세션에 쿠키 적용
                    for name, value in cookies.items():
                        self.session.cookies.set(name, value)

                    self._session_info = SessionInfo(
                        cookies=cookies,
                        last_login=datetime.now() - timedelta(hours=1)  # 1시간 전으로 설정
                    )

                    logger.info("기존 쿠키 파일을 로드했습니다.")
                    return True
            except Exception as e:
                logger.warning(f"기존 쿠키 로드 실패: {e}")

        return False

    def _save_session(self, cookies: Dict[str, str]):
        """세션 저장"""
        try:
            data = {
                "cookies": cookies,
                "last_login": datetime.now().isoformat()
            }
            self.COOKIE_PATH.write_text(json.dumps(data, indent=2))
            logger.info("세션을 저장했습니다.")
        except Exception as e:
            logger.warning(f"세션 저장 실패: {e}")

    def _validate_session(self) -> bool:
        """세션 유효성 검증 (실제 API 호출로 체크)"""
        try:
            # 간단한 API 호출로 세션 유효성 체크
            resp = self.session.post(
                "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd",
                data={
                    "bld": "dbms/MDC/STAT/standard/MDCSTAT03501",
                    "mktId": "STK",
                    "trdDd": datetime.now().strftime("%Y%m%d"),
                },
                timeout=10
            )

            # 응답이 비어있거나 HTML인 경우 (로그인 필요)
            content_type = resp.headers.get("Content-Type", "")
            if "text/html" in content_type:
                logger.info("HTML 응답 - 로그인 필요")
                return False

            if not resp.text.strip():
                logger.info("빈 응답 - 로그인 필요")
                return False

            try:
                data = resp.json()
            except:
                logger.info("JSON 파싱 실패 - 로그인 필요")
                return False

            # 로그아웃 상태 체크
            if isinstance(data, dict) and data.get("RESULT") == "LOGOUT":
                return False

            # 데이터가 있으면 성공
            if "output" in data or "OutBlock_1" in data:
                return True

            # 빈 데이터도 성공으로 처리 (휴일 등)
            if isinstance(data, dict):
                return True

            return False

        except Exception as e:
            logger.warning(f"세션 검증 실패: {e}")
            return False

    def login(self, force: bool = False) -> bool:
        """
        카카오 로그인

        Args:
            force: True면 기존 세션 무시하고 재로그인

        Returns:
            로그인 성공 여부

        Raises:
            KRX2FARequiredError: 2차인증이 활성화된 경우
        """
        # 기존 세션 체크
        if not force and self._load_session():
            if self._validate_session():
                logger.info("기존 세션이 유효합니다.")
                return True
            logger.info("기존 세션이 만료되어 재로그인합니다.")

        # Playwright로 로그인
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(self._login_async())
        finally:
            loop.close()

    async def _login_async(self) -> bool:
        """비동기 로그인 처리"""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            raise KRXAuthError(
                "playwright가 설치되지 않았습니다.\n"
                "'pip install playwright && playwright install chromium'을 실행하세요."
            )

        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(headless=self.headless)

        context = await self._browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            locale="ko-KR",
        )

        page = await context.new_page()

        try:
            # KRX 로그인 페이지
            login_url = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001.cmd"
            await page.goto(login_url, wait_until="networkidle", timeout=30000)
            await asyncio.sleep(2)

            # iframe에서 카카오 로그인 버튼 클릭
            iframe = await page.query_selector('iframe')
            if not iframe:
                raise KRXAuthError("로그인 iframe을 찾을 수 없습니다.")

            frame = await iframe.content_frame()
            kakao_btn = await frame.wait_for_selector(
                'a.ms-kakao, a:has-text("카카오로 로그인")',
                timeout=10000
            )
            await kakao_btn.click()

            # 카카오 로그인 페이지 대기
            await page.wait_for_url("**/accounts.kakao.com/**", timeout=15000)
            await page.wait_for_load_state("networkidle")
            await asyncio.sleep(1)

            # 아이디/비밀번호 입력
            await page.fill('input[name="loginId"], input#loginId', self.kakao_id)
            await asyncio.sleep(0.3)
            await page.fill('input[name="password"], input#password', self.kakao_pw)
            await asyncio.sleep(0.3)

            # 로그인 버튼 클릭
            await page.click('button[type="submit"], button.submit')
            logger.info("로그인 버튼 클릭됨. 2FA 확인 대기 중...")

            # 로그인 결과 대기 (최대 120초 - 2FA 확인 시간 포함)
            two_fa_detected = False
            krx_redirected = False
            for i in range(120):
                await asyncio.sleep(1)
                current_url = page.url

                # KRX로 리다이렉트 성공
                if current_url.startswith("http://data.krx.co.kr") or \
                   current_url.startswith("https://data.krx.co.kr"):
                    logger.info("KRX 로그인 성공!")
                    krx_redirected = True
                    break

                if i % 10 == 0:
                    logger.info(f"2FA 확인 대기 중... ({i}초)")

                # "계속하기" 버튼 처리 (2FA 후 나타남)
                try:
                    continue_btn = await page.query_selector('button:has-text("계속하기")')
                    if continue_btn:
                        logger.info("'계속하기' 버튼 발견, 클릭...")
                        await continue_btn.click()
                        await asyncio.sleep(2)
                        continue
                except:
                    pass

                # 동의 화면 처리
                try:
                    agree_btn = await page.query_selector(
                        'button:has-text("동의하고 계속하기"), button:has-text("전체 동의")'
                    )
                    if agree_btn:
                        logger.info("동의 버튼 클릭...")
                        await agree_btn.click()
                        await asyncio.sleep(2)
                except:
                    pass

            if not krx_redirected:
                # 2차인증 화면 감지
                try:
                    tfa_indicators = [
                        'text="카카오톡으로 인증"',
                        'text="인증 요청"',
                        'text="본인확인"',
                        'text="2단계 인증"',
                    ]
                    for indicator in tfa_indicators:
                        elem = await page.query_selector(indicator)
                        if elem:
                            two_fa_detected = True
                            break
                except:
                    pass

            # 2차인증 감지 시 에러 발생
            if two_fa_detected:
                await self._cleanup_browser()
                raise KRX2FARequiredError()

            # 로그인 성공 확인
            if not krx_redirected:
                current_url = page.url
                await self._cleanup_browser()
                raise KRXAuthError(
                    f"로그인 실패. 2FA 확인 또는 인증 정보를 확인하세요.\n"
                    f"현재 URL: {current_url[:100]}..."
                )

            # 쿠키 추출 및 저장
            cookies = await context.cookies()
            cookie_dict = {}

            # requests 세션에 쿠키 적용 (domain, path 포함해야 정상 동작)
            for cookie in cookies:
                name = cookie["name"]
                value = cookie["value"]
                domain = cookie.get("domain", "")
                path = cookie.get("path", "/")

                self.session.cookies.set(
                    name, value,
                    domain=domain,
                    path=path
                )
                cookie_dict[name] = value

            self._session_info = SessionInfo(
                cookies=cookie_dict,
                last_login=datetime.now()
            )

            self._save_session(cookie_dict)

            logger.info("카카오 로그인 성공")
            return True

        except KRX2FARequiredError:
            raise
        except Exception as e:
            logger.error(f"로그인 실패: {e}")
            raise KRXAuthError(f"로그인 실패: {e}")
        finally:
            await self._cleanup_browser()

    async def _cleanup_browser(self):
        """브라우저 정리"""
        if self._browser:
            await self._browser.close()
            self._browser = None
        if self._playwright:
            await self._playwright.stop()
            self._playwright = None

    def check_session(self) -> bool:
        """
        세션 상태 체크 및 필요시 재로그인

        Returns:
            세션 유효 여부
        """
        if not self.is_logged_in:
            return self.login()

        if not self._validate_session():
            logger.info("세션이 만료되어 재로그인합니다.")
            return self.login(force=True)

        return True


class KRXDataClient:
    """
    KRX 데이터 클라이언트

    pykrx와 동일한 인터페이스로 KRX Data Marketplace에서 데이터를 조회합니다.
    """

    API_URL = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"

    # bld 파라미터 (pykrx 분석 결과)
    BLD = {
        # 종목 검색
        "finder_stkisu": "dbms/comm/finder/finder_stkisu",
        # 개별종목 시세 (OHLCV)
        "ohlcv": "dbms/MDC/STAT/standard/MDCSTAT01701",
        # 전종목 시세
        "ohlcv_all": "dbms/MDC/STAT/standard/MDCSTAT01501",
        # PER/PBR - 전종목
        "fundamental_all": "dbms/MDC/STAT/standard/MDCSTAT03501",
        # PER/PBR - 개별종목 기간조회
        "fundamental": "dbms/MDC/STAT/standard/MDCSTAT03502",
        # 투자자별 거래 - 기간합계
        "investor_summary": "dbms/MDC/STAT/standard/MDCSTAT02301",
        # 투자자별 거래 - 일별추이 (일반: 5개 투자자 유형)
        "investor_daily": "dbms/MDC/STAT/standard/MDCSTAT02302",
        # 투자자별 거래 - 일별추이 (상세: 12개 투자자 유형)
        "investor_daily_detail": "dbms/MDC/STAT/standard/MDCSTAT02303",
        # 지수 시세
        "index_ohlcv": "dbms/MDC/STAT/standard/MDCSTAT00301",
        # 지수 검색
        "finder_index": "dbms/comm/finder/finder_equidx",
    }

    # 시장 코드 매핑
    MARKET_CODE = {
        "KOSPI": "STK",
        "KOSDAQ": "KSQ",
        "KONEX": "KNX",
        "ALL": "ALL",
    }

    def __init__(
        self,
        kakao_id: Optional[str] = None,
        kakao_pw: Optional[str] = None,
        headless: bool = True,
        auto_login: bool = True,
    ):
        """
        클라이언트 초기화

        Args:
            kakao_id: 카카오 아이디
            kakao_pw: 카카오 비밀번호
            headless: 헤드리스 브라우저 모드
            auto_login: 자동 로그인 여부
        """
        self._auth_manager = KakaoAuthManager(
            kakao_id=kakao_id,
            kakao_pw=kakao_pw,
            headless=headless,
        )

        # ticker → ISIN 캐시
        self._isin_cache: Dict[str, str] = {}
        self._isin_cache_date: Optional[str] = None

        if auto_login:
            self._auth_manager.login()

    @property
    def session(self) -> requests.Session:
        """requests 세션"""
        return self._auth_manager.session

    def _ensure_session(self):
        """세션 유효성 확인"""
        if not self._auth_manager.is_logged_in:
            if not self._auth_manager.login():
                raise KRXSessionExpiredError("세션을 복구할 수 없습니다.")

    def _request(
        self,
        bld: str,
        params: Dict[str, Any],
        output_key: str = "output"
    ) -> List[Dict[str, Any]]:
        """
        KRX API 요청

        Args:
            bld: bld 파라미터
            params: 요청 파라미터
            output_key: 응답에서 데이터를 추출할 키

        Returns:
            응답 데이터 리스트
        """
        self._ensure_session()

        request_data = {"bld": bld, **params}

        try:
            resp = self.session.post(self.API_URL, data=request_data, timeout=30)
            resp.raise_for_status()

            data = resp.json()

            # 로그아웃 상태 체크
            if isinstance(data, dict):
                if data.get("RESULT") == "LOGOUT":
                    raise KRXSessionExpiredError("세션이 만료되었습니다.")

                # 데이터 추출
                if output_key in data:
                    return data[output_key]
                elif "OutBlock_1" in data:
                    return data["OutBlock_1"]
                elif "block1" in data:
                    return data["block1"]
                else:
                    return [data] if data else []
            elif isinstance(data, list):
                return data
            else:
                return []

        except requests.exceptions.RequestException as e:
            raise KRXDataError(f"API 요청 실패: {e}")

    # =========================================================================
    # 종목 검색
    # =========================================================================

    @retry_on_session_expired()
    def get_market_ticker_list(
        self,
        date: Optional[str] = None,
        market: str = "ALL"
    ) -> List[str]:
        """
        종목 코드 리스트 조회

        Args:
            date: 기준일자 (YYYYMMDD), None이면 오늘
            market: 시장 (KOSPI/KOSDAQ/KONEX/ALL)

        Returns:
            종목 코드 리스트
        """
        df = self._get_ticker_info(market)
        return df["short_code"].tolist() if not df.empty else []

    @retry_on_session_expired()
    def get_market_ticker_name(self, date: Optional[str] = None, market: str = "ALL") -> Dict[str, str]:
        """
        종목코드-종목명 매핑

        Args:
            date: 기준일자 (미사용, 호환성용)
            market: 시장

        Returns:
            {종목코드: 종목명} 딕셔너리
        """
        df = self._get_ticker_info(market)
        if df.empty:
            return {}
        return dict(zip(df["short_code"], df["codeName"]))

    def _get_ticker_info(self, market: str = "ALL") -> DataFrame:
        """종목 정보 조회 (내부용)"""
        mktsel = self.MARKET_CODE.get(market.upper(), "ALL")

        items = self._request(
            self.BLD["finder_stkisu"],
            {"locale": "ko_KR", "mktsel": mktsel, "searchText": "", "typeNo": 0}
        )

        if not items:
            return DataFrame()

        return DataFrame(items)

    def _build_isin_cache(self, date: str):
        """ISIN 캐시 구축"""
        if self._isin_cache and self._isin_cache_date == date:
            return

        items = self._request(
            self.BLD["fundamental_all"],
            {"mktId": "ALL", "trdDd": date}
        )

        self._isin_cache = {}
        for item in items:
            ticker = item.get("ISU_SRT_CD", "")
            isin = item.get("ISU_CD", "")
            if ticker and isin:
                self._isin_cache[ticker] = isin

        self._isin_cache_date = date
        logger.info(f"ISIN 캐시 구축 완료: {len(self._isin_cache)}개 종목")

    def _get_isin(self, ticker: str, date: str) -> Optional[str]:
        """ticker에서 ISIN 조회"""
        self._build_isin_cache(date)
        return self._isin_cache.get(ticker)

    # =========================================================================
    # OHLCV (시세)
    # =========================================================================

    @retry_on_session_expired()
    def get_market_ohlcv(
        self,
        fromdate: str,
        todate: str,
        ticker: str,
        adjusted: bool = True
    ) -> DataFrame:
        """
        개별종목 OHLCV 조회 (pykrx 호환)

        Args:
            fromdate: 시작일 (YYYYMMDD)
            todate: 종료일 (YYYYMMDD)
            ticker: 종목코드 (6자리)
            adjusted: 수정주가 여부

        Returns:
            DataFrame: 날짜별 OHLCV
                - Open, High, Low, Close, Volume
        """
        isin = self._get_isin(ticker, todate)
        if not isin:
            raise KRXDataError(f"종목을 찾을 수 없습니다: {ticker}")

        items = self._request(
            self.BLD["ohlcv"],
            {
                "isuCd": isin,
                "strtDd": fromdate,
                "endDd": todate,
                "adjStkPrc": 2 if adjusted else 1,  # 2: 수정주가, 1: 단순주가
            }
        )

        if not items:
            return DataFrame()

        df = DataFrame(items)

        # 컬럼 매핑 (pykrx 형식)
        column_map = {
            "TRD_DD": "날짜",
            "TDD_OPNPRC": "시가",
            "TDD_HGPRC": "고가",
            "TDD_LWPRC": "저가",
            "TDD_CLSPRC": "종가",
            "ACC_TRDVOL": "거래량",
            "ACC_TRDVAL": "거래대금",
            "MKTCAP": "시가총액",
        }
        df = df.rename(columns=column_map)

        # pykrx 영문 컬럼명으로 변환
        eng_map = {
            "시가": "Open",
            "고가": "High",
            "저가": "Low",
            "종가": "Close",
            "거래량": "Volume",
            "거래대금": "Amount",
            "시가총액": "MarketCap",
        }
        df = df.rename(columns=eng_map)

        # 숫자 변환
        numeric_cols = ["Open", "High", "Low", "Close", "Volume", "Amount", "MarketCap"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(",", ""),
                    errors="coerce"
                )

        # 날짜 인덱스
        if "날짜" in df.columns:
            df["날짜"] = pd.to_datetime(df["날짜"], format="%Y/%m/%d")
            df = df.set_index("날짜")
            df.index.name = None
            df = df.sort_index()

        # pykrx와 동일한 컬럼만 반환
        result_cols = ["Open", "High", "Low", "Close", "Volume", "Amount", "MarketCap"]
        available = [c for c in result_cols if c in df.columns]

        return df[available] if available else df

    # =========================================================================
    # 시가총액
    # =========================================================================

    @retry_on_session_expired()
    def get_market_cap(
        self,
        fromdate: str,
        todate: str,
        ticker: str
    ) -> DataFrame:
        """
        시가총액 조회 (pykrx 호환)

        Args:
            fromdate: 시작일 (YYYYMMDD)
            todate: 종료일 (YYYYMMDD)
            ticker: 종목코드

        Returns:
            DataFrame: 시가총액, 거래량, 거래대금, 상장주식수
        """
        df = self.get_market_ohlcv(fromdate, todate, ticker)

        if df.empty:
            return df

        # 시가총액 관련 컬럼만 반환
        cols = ["MarketCap", "Volume", "Amount"]
        available = [c for c in cols if c in df.columns]

        return df[available] if available else df

    # =========================================================================
    # PER/PBR/배당수익률 (Fundamental)
    # =========================================================================

    @retry_on_session_expired()
    def get_market_fundamental(
        self,
        fromdate: str,
        todate: str,
        ticker: str
    ) -> DataFrame:
        """
        PER/PBR/배당수익률 조회 (pykrx 호환)

        Args:
            fromdate: 시작일 (YYYYMMDD)
            todate: 종료일 (YYYYMMDD)
            ticker: 종목코드

        Returns:
            DataFrame: BPS, PER, PBR, EPS, DIV, DPS
        """
        isin = self._get_isin(ticker, todate)
        if not isin:
            raise KRXDataError(f"종목을 찾을 수 없습니다: {ticker}")

        items = self._request(
            self.BLD["fundamental"],
            {
                "isuCd": isin,
                "mktId": "ALL",
                "strtDd": fromdate,
                "endDd": todate,
            }
        )

        if not items:
            return DataFrame()

        df = DataFrame(items)

        # 컬럼 매핑 (pykrx 형식)
        column_map = {
            "TRD_DD": "날짜",
            "TDD_CLSPRC": "종가",
            "EPS": "EPS",
            "PER": "PER",
            "BPS": "BPS",
            "PBR": "PBR",
            "DPS": "DPS",
            "DVD_YLD": "DIV",
        }
        df = df.rename(columns=column_map)

        # 숫자 변환
        numeric_cols = ["종가", "EPS", "PER", "BPS", "PBR", "DPS", "DIV"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(",", ""),
                    errors="coerce"
                )

        # 날짜 인덱스
        if "날짜" in df.columns:
            df["날짜"] = pd.to_datetime(df["날짜"], format="%Y/%m/%d")
            df = df.set_index("날짜")
            df.index.name = None
            df = df.sort_index()

        # pykrx와 동일한 컬럼만 반환
        result_cols = ["BPS", "PER", "PBR", "EPS", "DIV", "DPS"]
        available = [c for c in result_cols if c in df.columns]

        return df[available] if available else df

    # =========================================================================
    # 투자자별 거래량
    # =========================================================================

    @retry_on_session_expired()
    def get_market_trading_volume_by_date(
        self,
        fromdate: str,
        todate: str,
        ticker: str,
        detail: bool = False
    ) -> DataFrame:
        """
        투자자별 거래량 조회 (pykrx 호환)

        Args:
            fromdate: 시작일 (YYYYMMDD)
            todate: 종료일 (YYYYMMDD)
            ticker: 종목코드
            detail: 상세 투자자 구분 여부
                   - False: 5개 유형 (기관합계, 기타법인, 개인, 외국인합계, 전체)
                   - True: 12개 유형 (금융투자, 보험, 투신, 사모, 은행, 기타금융, 연기금, 기타법인, 개인, 외국인, 기타외국인, 전체)

        Returns:
            DataFrame: 투자자별 순매수량
        """
        isin = self._get_isin(ticker, todate)
        if not isin:
            raise KRXDataError(f"종목을 찾을 수 없습니다: {ticker}")

        # detail 여부에 따라 다른 bld 사용
        bld_key = "investor_daily_detail" if detail else "investor_daily"

        items = self._request(
            self.BLD[bld_key],
            {
                "isuCd": isin,
                "strtDd": fromdate,
                "endDd": todate,
                "inqTpCd": 2,
                "trdVolVal": 1,  # 거래량
                "askBid": 3,     # 순매수
            }
        )

        if not items:
            return DataFrame()

        df = DataFrame(items)

        # 컬럼 매핑 (detail 여부에 따라 다름)
        if detail:
            # 상세: 12개 투자자 유형
            column_map = {
                "TRD_DD": "날짜",
                "TRDVAL1": "금융투자",
                "TRDVAL2": "보험",
                "TRDVAL3": "투신",
                "TRDVAL4": "사모",
                "TRDVAL5": "은행",
                "TRDVAL6": "기타금융",
                "TRDVAL7": "연기금",
                "TRDVAL8": "기타법인",
                "TRDVAL9": "개인",
                "TRDVAL10": "외국인",
                "TRDVAL11": "기타외국인",
                "TRDVAL_TOT": "전체",
            }
        else:
            # 일반: 5개 투자자 유형
            column_map = {
                "TRD_DD": "날짜",
                "TRDVAL1": "기관합계",
                "TRDVAL2": "기타법인",
                "TRDVAL3": "개인",
                "TRDVAL4": "외국인합계",
                "TRDVAL_TOT": "전체",
            }

        df = df.rename(columns=column_map)

        # 숫자 변환
        numeric_cols = list(column_map.values())[1:]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(",", ""),
                    errors="coerce"
                )

        # 날짜 인덱스
        if "날짜" in df.columns:
            df["날짜"] = pd.to_datetime(df["날짜"], format="%Y/%m/%d")
            df = df.set_index("날짜")
            df.index.name = None
            df = df.sort_index()

        return df

    # =========================================================================
    # 지수 OHLCV
    # =========================================================================

    @retry_on_session_expired()
    def get_index_ohlcv(
        self,
        fromdate: str,
        todate: str,
        ticker: str,
        freq: str = "d"
    ) -> DataFrame:
        """
        지수 OHLCV 조회 (pykrx 호환)

        Args:
            fromdate: 시작일 (YYYYMMDD)
            todate: 종료일 (YYYYMMDD)
            ticker: 지수 코드 (예: 1001=KOSPI, 2001=KOSDAQ)
            freq: 빈도 (d/m/y) - 현재 d만 지원

        Returns:
            DataFrame: 지수 OHLCV
        """
        # pykrx 지수 티커 형식: 1xxx=KOSPI, 2xxx=KOSDAQ
        # API 파라미터:
        #   indIdx: 그룹 ID (1=KOSPI, 2=KOSDAQ 등)
        #   indIdx2: 지수 코드 (001=코스피/코스닥, 028=KOSPI 200 등)
        ticker_str = str(ticker)

        ind_idx = ticker_str[0]    # 첫 번째 자리: 그룹 ID
        idx_code = ticker_str[1:]  # 나머지: 지수 코드

        items = self._request(
            self.BLD["index_ohlcv"],
            {
                "indIdx2": idx_code,
                "indIdx": ind_idx,
                "strtDd": fromdate,
                "endDd": todate,
            }
        )

        if not items:
            return DataFrame()

        df = DataFrame(items)

        # 컬럼 매핑
        column_map = {
            "TRD_DD": "날짜",
            "OPNPRC_IDX": "시가",
            "HGPRC_IDX": "고가",
            "LWPRC_IDX": "저가",
            "CLSPRC_IDX": "종가",
            "ACC_TRDVOL": "거래량",
            "ACC_TRDVAL": "거래대금",
        }
        df = df.rename(columns=column_map)

        # pykrx 영문 컬럼
        eng_map = {
            "시가": "Open",
            "고가": "High",
            "저가": "Low",
            "종가": "Close",
            "거래량": "Volume",
            "거래대금": "Amount",
        }
        df = df.rename(columns=eng_map)

        # 숫자 변환
        numeric_cols = ["Open", "High", "Low", "Close", "Volume", "Amount"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(",", ""),
                    errors="coerce"
                )

        # 날짜 인덱스
        if "날짜" in df.columns:
            df["날짜"] = pd.to_datetime(df["날짜"], format="%Y/%m/%d")
            df = df.set_index("날짜")
            df.index.name = None
            df = df.sort_index()

        # pykrx와 동일한 컬럼만 반환
        result_cols = ["Open", "High", "Low", "Close", "Volume", "Amount"]
        available = [c for c in result_cols if c in df.columns]

        return df[available] if available else df

    # =========================================================================
    # 유틸리티
    # =========================================================================

    def get_nearest_business_day(self, date: Optional[str] = None) -> str:
        """
        가장 가까운 영업일 조회

        Args:
            date: 기준일 (YYYYMMDD), None이면 오늘

        Returns:
            영업일 (YYYYMMDD)
        """
        if date:
            dt = datetime.strptime(date, "%Y%m%d")
        else:
            dt = datetime.now()

        # 주말이면 금요일로
        weekday = dt.weekday()
        if weekday == 5:  # 토요일
            dt = dt - timedelta(days=1)
        elif weekday == 6:  # 일요일
            dt = dt - timedelta(days=2)

        return dt.strftime("%Y%m%d")

    def close(self):
        """리소스 정리"""
        pass  # 현재는 특별히 정리할 것 없음


# =============================================================================
# 테스트
# =============================================================================

def test_client():
    """클라이언트 테스트"""
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    print("=" * 60)
    print("KRX Data Client 테스트")
    print("=" * 60)

    try:
        client = KRXDataClient()

        # 종목 코드 조회
        print("\n[1] 종목코드 조회")
        ticker_map = client.get_market_ticker_name(market="KOSPI")
        print(f"KOSPI 종목 수: {len(ticker_map)}")

        # OHLCV 조회
        print("\n[2] 삼성전자 OHLCV (2024-12-01 ~ 2024-12-20)")
        df = client.get_market_ohlcv("20241201", "20241220", "005930")
        print(df.head())

        # PER/PBR 조회
        print("\n[3] 삼성전자 PER/PBR (2024-12-01 ~ 2024-12-20)")
        df = client.get_market_fundamental("20241201", "20241220", "005930")
        print(df.head())

        # 투자자별 거래량
        print("\n[4] 삼성전자 투자자별 거래량 (2024-12-01 ~ 2024-12-20)")
        df = client.get_market_trading_volume_by_date("20241201", "20241220", "005930")
        print(df.head())

        # 지수 OHLCV
        print("\n[5] KOSPI 지수 (2024-12-01 ~ 2024-12-20)")
        df = client.get_index_ohlcv("20241201", "20241220", "1001")
        print(df.head())

        print("\n" + "=" * 60)
        print("모든 테스트 완료!")
        print("=" * 60)

    except KRX2FARequiredError as e:
        print(f"\n[ERROR] {e}")
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_client()
