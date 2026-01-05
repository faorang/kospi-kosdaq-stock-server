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
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Dict, Any, Optional, List, Callable, TypeVar
from dataclasses import dataclass

from holidays.countries import KR

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
                    time.sleep(delay * (attempt + 1))  # exponential backoff
                    try:
                        # 세션 파일 삭제 후 재로그인
                        self._auth_manager._cleanup_session_files()
                        self._auth_manager.login(force=True)
                    except Exception as login_error:
                        logger.error(f"재로그인 실패: {login_error}")
                        if attempt == max_retries - 1:
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
    SESSION_REFRESH_THRESHOLD = timedelta(hours=3)  # 이 시간 이후면 선제적 갱신
    VALIDATION_SKIP_THRESHOLD = timedelta(minutes=5)  # 이 시간 내 검증됐으면 재검증 생략

    # Playwright 타임아웃 설정 (운영 안정성을 위해 충분히 길게)
    PAGE_LOAD_TIMEOUT = 60000  # 60초
    LOGIN_WAIT_TIMEOUT = 30000  # 30초
    MAX_LOGIN_RETRIES = 3  # 로그인 재시도 횟수

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
        self._last_validated: Optional[datetime] = None  # 마지막 세션 검증 시간 (파일에서 로드)
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
                # 파일 수정 시간도 확인 (race condition 대응)
                file_mtime = datetime.fromtimestamp(self.COOKIE_PATH.stat().st_mtime)
                file_is_fresh = datetime.now() - file_mtime < self.VALIDATION_SKIP_THRESHOLD

                data = json.loads(self.COOKIE_PATH.read_text())
                cookies = data.get("cookies", {})
                last_login_str = data.get("last_login")
                last_validated_str = data.get("last_validated")  # 마지막 검증 시간

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

                        # 마지막 검증 시간 로드 (파일 수정 시간으로 대체 가능)
                        if last_validated_str:
                            self._last_validated = datetime.fromisoformat(last_validated_str)
                        elif file_is_fresh:
                            # last_validated 없어도 파일이 최근 수정됐으면 신뢰
                            self._last_validated = file_mtime
                            logger.info(f"파일 수정 시간으로 세션 신뢰: {file_mtime}")

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

    def _save_session(self, cookies: Dict[str, str], update_validated: bool = True):
        """세션 저장"""
        try:
            now = datetime.now()
            data = {
                "cookies": cookies,
                "last_login": now.isoformat(),
                "last_validated": now.isoformat() if update_validated else None
            }
            self.COOKIE_PATH.write_text(json.dumps(data, indent=2))
            if update_validated:
                self._last_validated = now
            logger.info("세션을 저장했습니다.")
        except Exception as e:
            logger.warning(f"세션 저장 실패: {e}")

    def _update_last_validated(self):
        """세션 파일의 last_validated만 업데이트 (검증 성공 시 호출)"""
        try:
            if not self.COOKIE_PATH.exists():
                return

            data = json.loads(self.COOKIE_PATH.read_text())
            now = datetime.now()
            data["last_validated"] = now.isoformat()
            self.COOKIE_PATH.write_text(json.dumps(data, indent=2))
            self._last_validated = now
            logger.debug("세션 검증 시간 업데이트")
        except Exception as e:
            logger.warning(f"세션 검증 시간 업데이트 실패: {e}")

    def _cleanup_session_files(self):
        """세션 파일 삭제 (손상/만료 시 호출)"""
        for path in [self.COOKIE_PATH, self.LEGACY_COOKIE_PATH]:
            try:
                if path.exists():
                    path.unlink()
                    logger.info(f"세션 파일 삭제: {path}")
            except Exception as e:
                logger.warning(f"세션 파일 삭제 실패: {path}, {e}")

        # 세션 정보 초기화
        self._session_info = None
        if self._session:
            self._session.cookies.clear()

    def _get_recent_business_day(self) -> str:
        """가장 최근 영업일 반환 (세션 검증용)"""
        kr_holidays = KR()
        dt = date.today()

        # 장 시작 전(09:00 이전)이면 전일부터 탐색
        if datetime.now().hour < 9:
            dt -= timedelta(days=1)

        # 최대 10일 전까지 탐색
        for _ in range(10):
            # 주말 체크
            if dt.weekday() >= 5:
                dt -= timedelta(days=1)
                continue
            # 공휴일 체크
            if dt in kr_holidays:
                dt -= timedelta(days=1)
                continue
            # 연말(12/31), 노동절(5/1) 체크
            if (dt.month == 12 and dt.day == 31) or (dt.month == 5 and dt.day == 1):
                dt -= timedelta(days=1)
                continue
            return dt.strftime("%Y%m%d")
            dt -= timedelta(days=1)

        return dt.strftime("%Y%m%d")

    def _validate_session(self) -> bool:
        """세션 유효성 검증 (실제 API 호출로 체크)"""
        try:
            # 가장 최근 영업일 계산 (장 시작 전/휴일에도 동작)
            check_date = self._get_recent_business_day()

            # 간단한 API 호출로 세션 유효성 체크
            resp = self.session.post(
                "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd",
                data={
                    "bld": "dbms/MDC/STAT/standard/MDCSTAT03501",
                    "mktId": "STK",
                    "trdDd": check_date,
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
        카카오 로그인 (자동 재시도 포함)

        Args:
            force: True면 기존 세션 무시하고 재로그인

        Returns:
            로그인 성공 여부

        Raises:
            KRX2FARequiredError: 2차인증이 활성화된 경우
        """
        # 기존 세션 체크
        if not force and self._load_session():
            # 최근 검증됐으면 재검증 생략 (다른 프로세스에서 검증한 경우 포함)
            if self._last_validated and datetime.now() - self._last_validated < self.VALIDATION_SKIP_THRESHOLD:
                logger.info(f"최근 검증된 세션 사용 (검증 시각: {self._last_validated})")
                return True

            if self._validate_session():
                self._update_last_validated()  # 검증 성공 시 파일에 기록
                logger.info("기존 세션이 유효합니다.")
                return True
            logger.info("기존 세션이 만료되어 재로그인합니다.")
            self._cleanup_session_files()

        # Playwright로 로그인 (재시도 로직 포함)
        last_error = None
        for attempt in range(self.MAX_LOGIN_RETRIES):
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    result = loop.run_until_complete(self._login_async())
                    if result:
                        return True
                finally:
                    loop.close()
            except KRX2FARequiredError:
                # 2FA 에러는 재시도해도 의미 없음
                raise
            except Exception as e:
                last_error = e
                logger.warning(f"로그인 시도 {attempt + 1}/{self.MAX_LOGIN_RETRIES} 실패: {e}")
                if attempt < self.MAX_LOGIN_RETRIES - 1:
                    wait_time = (attempt + 1) * 5  # 5초, 10초, 15초...
                    logger.info(f"{wait_time}초 후 재시도...")
                    time.sleep(wait_time)
                    self._cleanup_session_files()

        raise KRXAuthError(f"로그인 실패 (최대 재시도 횟수 초과): {last_error}")

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
            await page.goto(login_url, wait_until="networkidle", timeout=self.PAGE_LOAD_TIMEOUT)
            await asyncio.sleep(2)

            # iframe에서 카카오 로그인 버튼 클릭
            iframe = await page.query_selector('iframe')
            if not iframe:
                raise KRXAuthError("로그인 iframe을 찾을 수 없습니다.")

            frame = await iframe.content_frame()
            kakao_btn = await frame.wait_for_selector(
                'a.ms-kakao, a:has-text("카카오로 로그인")',
                timeout=self.LOGIN_WAIT_TIMEOUT
            )
            await kakao_btn.click()

            # 카카오 로그인 페이지 대기
            await page.wait_for_url("**/accounts.kakao.com/**", timeout=self.LOGIN_WAIT_TIMEOUT)
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

    def _needs_refresh(self) -> bool:
        """세션 선제적 갱신이 필요한지 확인"""
        if not self._session_info:
            return True

        elapsed = datetime.now() - self._session_info.last_login
        return elapsed >= self.SESSION_REFRESH_THRESHOLD

    def check_session(self) -> bool:
        """
        세션 상태 체크 및 필요시 재로그인

        Returns:
            세션 유효 여부
        """
        if not self.is_logged_in:
            return self.login()

        # 선제적 갱신: 만료되기 전에 미리 갱신
        if self._needs_refresh():
            logger.info("세션 만료 예정, 선제적 갱신 시도...")
            if not self._validate_session():
                logger.info("세션이 만료되어 재로그인합니다.")
                self._cleanup_session_files()
                return self.login(force=True)

        if not self._validate_session():
            logger.info("세션이 만료되어 재로그인합니다.")
            self._cleanup_session_files()
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
        """세션 유효성 확인 (freshness 체크로 불필요한 검증 방지)"""
        global _last_session_check_time

        # 최근에 세션 검증이 성공했으면 생략 (프로세스 내 재사용)
        if _last_session_check_time and datetime.now() - _last_session_check_time < FRESH_SESSION_THRESHOLD:
            return  # 최근 검증 성공, 생략

        if not self._auth_manager.is_logged_in:
            if not self._auth_manager.login():
                raise KRXSessionExpiredError("세션을 복구할 수 없습니다.")

        # 검증 성공 시간 기록
        _last_session_check_time = datetime.now()

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

        except requests.exceptions.HTTPError as e:
            # 400 Bad Request는 세션 만료일 가능성이 있으므로 재시도 유도
            if e.response is not None and e.response.status_code == 400:
                logger.warning(f"400 Bad Request 발생, 세션 문제일 수 있음: {e}")
                raise KRXSessionExpiredError(f"세션 문제로 추정되는 400 에러: {e}")
            raise KRXDataError(f"API 요청 실패: {e}")
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
        # ISIN 캐시는 가장 최근 영업일 기준으로 구축 (장 시작 전/휴일에도 동작)
        cache_date = self.get_nearest_business_day(date)
        self._build_isin_cache(cache_date)
        isin = self._isin_cache.get(ticker)

        # fundamental_all에서 못 찾으면 finder_stkisu에서 검색 (외국 상장사 등)
        if not isin:
            items = self._request(
                self.BLD["finder_stkisu"],
                {"locale": "ko_KR", "mktsel": "ALL", "searchText": ticker, "typeNo": 0}
            )
            for item in items:
                if item.get("short_code") == ticker:
                    isin = item.get("full_code")
                    # 캐시에 추가
                    if isin:
                        self._isin_cache[ticker] = isin
                        logger.debug(f"finder_stkisu에서 ISIN 찾음: {ticker} -> {isin}")
                    break

        return isin

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
    # 전체 종목 조회 (pykrx 호환)
    # =========================================================================

    @retry_on_session_expired()
    def get_market_ohlcv_by_ticker(self, date: str, market: str = "ALL") -> DataFrame:
        """
        특정일 전체 종목의 OHLCV 조회 (pykrx 호환)

        Args:
            date: 조회일 (YYYYMMDD)
            market: 시장구분 ("ALL", "KOSPI", "KOSDAQ", "KONEX")

        Returns:
            DataFrame: 종목코드 인덱스, OHLCV 컬럼
        """
        # 가장 최근 영업일로 변환 (장 시작 전/휴일에도 동작)
        query_date = self.get_nearest_business_day(date)

        market_map = {
            "ALL": "ALL",
            "KOSPI": "STK",
            "KOSDAQ": "KSQ",
            "KONEX": "KNX",
        }
        mkt_id = market_map.get(market.upper(), "ALL")

        items = self._request(
            "dbms/MDC/STAT/standard/MDCSTAT01501",
            {
                "mktId": mkt_id,
                "trdDd": query_date,
            }
        )

        if not items:
            return DataFrame()

        df = DataFrame(items)

        # 컬럼 매핑
        column_map = {
            "ISU_SRT_CD": "티커",
            "TDD_OPNPRC": "시가",
            "TDD_HGPRC": "고가",
            "TDD_LWPRC": "저가",
            "TDD_CLSPRC": "종가",
            "ACC_TRDVOL": "거래량",
            "ACC_TRDVAL": "거래대금",
        }
        df = df.rename(columns=column_map)

        # 숫자 변환
        numeric_cols = ["시가", "고가", "저가", "종가", "거래량", "거래대금"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(",", ""),
                    errors="coerce"
                )

        # 티커 인덱스
        if "티커" in df.columns:
            df = df.set_index("티커")

        # 필요한 컬럼만 반환
        result_cols = ["시가", "고가", "저가", "종가", "거래량", "거래대금"]
        available = [c for c in result_cols if c in df.columns]

        return df[available] if available else df

    @retry_on_session_expired()
    def get_market_cap_by_ticker(self, date: str, market: str = "ALL") -> DataFrame:
        """
        특정일 전체 종목의 시가총액 조회 (pykrx 호환)

        Args:
            date: 조회일 (YYYYMMDD)
            market: 시장구분 ("ALL", "KOSPI", "KOSDAQ", "KONEX")

        Returns:
            DataFrame: 종목코드 인덱스, 시가총액/거래량/거래대금/상장주식수 컬럼
        """
        # 가장 최근 영업일로 변환 (장 시작 전/휴일에도 동작)
        query_date = self.get_nearest_business_day(date)

        market_map = {
            "ALL": "ALL",
            "KOSPI": "STK",
            "KOSDAQ": "KSQ",
            "KONEX": "KNX",
        }
        mkt_id = market_map.get(market.upper(), "ALL")

        # MDCSTAT01501 (전종목 시세) endpoint 사용 - MKTCAP 포함
        items = self._request(
            self.BLD["ohlcv_all"],
            {
                "mktId": mkt_id,
                "trdDd": query_date,
            }
        )

        if not items:
            return DataFrame()

        df = DataFrame(items)

        # 컬럼 매핑 (pykrx 형식)
        column_map = {
            "ISU_SRT_CD": "티커",
            "MKTCAP": "시가총액",
            "ACC_TRDVOL": "거래량",
            "ACC_TRDVAL": "거래대금",
            "LIST_SHRS": "상장주식수",
        }
        df = df.rename(columns=column_map)

        # 숫자 변환
        numeric_cols = ["시가총액", "거래량", "거래대금", "상장주식수"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(",", ""),
                    errors="coerce"
                )

        # 티커 인덱스
        if "티커" in df.columns:
            df = df.set_index("티커")

        # 필요한 컬럼만 반환
        result_cols = ["시가총액", "거래량", "거래대금", "상장주식수"]
        available = [c for c in result_cols if c in df.columns]

        return df[available] if available else df

    def get_market_ticker_list(self, date: Optional[str] = None, market: str = "KOSPI") -> List[str]:
        """
        티커 목록 조회 (pykrx 호환)

        Args:
            date: 조회일 (YYYYMMDD), None이면 최근 영업일
            market: 시장구분 ("KOSPI", "KOSDAQ", "KONEX", "ALL")

        Returns:
            List[str]: 티커 코드 리스트
        """
        if date is None:
            date = self.get_nearest_business_day()

        tickers = self.get_market_ticker_name(date=date, market=market)
        return list(tickers.keys())

    @retry_on_session_expired()
    def get_market_trading_value_by_investor(
        self,
        fromdate: str,
        todate: str,
        ticker: str,
        detail: bool = False
    ) -> DataFrame:
        """
        투자자별 거래대금 조회 (pykrx 호환)

        Args:
            fromdate: 시작일 (YYYYMMDD)
            todate: 종료일 (YYYYMMDD)
            ticker: 종목코드 (6자리)
            detail: True면 12개 투자자, False면 5개 합산

        Returns:
            DataFrame: 날짜별 투자자별 순매수금액
        """
        isin = self._get_isin(ticker, todate)
        if not isin:
            raise KRXDataError(f"종목을 찾을 수 없습니다: {ticker}")

        # detail 여부에 따라 다른 BLD 사용
        bld = self.BLD["trading_volume_detail"] if detail else self.BLD["trading_volume"]

        items = self._request(
            bld,
            {
                "isuCd": isin,
                "strtDd": fromdate,
                "endDd": todate,
            }
        )

        if not items:
            return DataFrame()

        df = DataFrame(items)

        # 컬럼 매핑 - 거래대금(ASK_TRDVAL: 매도, BID_TRDVAL: 매수)
        date_col = "TRD_DD"
        if date_col not in df.columns:
            return DataFrame()

        # 순매수금액 계산 (매수 - 매도)
        result_data = []
        for _, row in df.iterrows():
            row_dict = {"날짜": row[date_col]}

            if detail:
                # 상세 (12개 투자자)
                investors = [
                    ("금융투자", "TRDVAL1"),
                    ("보험", "TRDVAL2"),
                    ("투신", "TRDVAL3"),
                    ("사모", "TRDVAL4"),
                    ("은행", "TRDVAL5"),
                    ("기타금융", "TRDVAL6"),
                    ("연기금", "TRDVAL7"),
                    ("기관합계", "TRDVAL_INST"),
                    ("기타법인", "TRDVAL8"),
                    ("개인", "TRDVAL9"),
                    ("외국인", "TRDVAL10"),
                    ("기타외국인", "TRDVAL11"),
                ]
            else:
                # 합산 (5개 투자자)
                investors = [
                    ("기관합계", "TRDVAL_INST"),
                    ("기타법인", "TRDVAL1"),
                    ("개인", "TRDVAL2"),
                    ("외국인합계", "TRDVAL3"),
                    ("전체", "TRDVAL4"),
                ]

            for name, col_prefix in investors:
                buy_col = f"BID_{col_prefix}" if f"BID_{col_prefix}" in row else col_prefix
                sell_col = f"ASK_{col_prefix}" if f"ASK_{col_prefix}" in row else None

                if buy_col in row:
                    buy = pd.to_numeric(str(row.get(buy_col, 0)).replace(",", ""), errors="coerce") or 0
                    sell = pd.to_numeric(str(row.get(sell_col, 0)).replace(",", ""), errors="coerce") or 0 if sell_col else 0
                    row_dict[name] = buy - sell

            result_data.append(row_dict)

        result_df = DataFrame(result_data)

        # 날짜 인덱스
        if "날짜" in result_df.columns:
            result_df["날짜"] = pd.to_datetime(result_df["날짜"], format="%Y/%m/%d")
            result_df = result_df.set_index("날짜")
            result_df.index.name = None
            result_df = result_df.sort_index()

        return result_df

    @retry_on_session_expired()
    def get_market_trading_value_by_date(
        self,
        fromdate: str,
        todate: str,
        ticker: str,
        on: str = "순매수"
    ) -> DataFrame:
        """
        일자별 거래대금 조회 (pykrx 호환)

        Args:
            fromdate: 시작일 (YYYYMMDD)
            todate: 종료일 (YYYYMMDD)
            ticker: 종목코드 (6자리)
            on: "매도", "매수", "순매수" 중 하나

        Returns:
            DataFrame: 날짜별 거래대금
        """
        # 투자자별 거래대금 조회 후 집계
        df = self.get_market_trading_value_by_investor(fromdate, todate, ticker)
        return df

    # =========================================================================
    # pykrx 호환 별칭
    # =========================================================================

    def get_market_ohlcv_by_date(
        self,
        fromdate: str,
        todate: str,
        ticker: str,
        adjusted: bool = True
    ) -> DataFrame:
        """pykrx 호환: get_market_ohlcv의 별칭"""
        return self.get_market_ohlcv(fromdate, todate, ticker, adjusted)

    def get_market_cap_by_date(
        self,
        fromdate: str,
        todate: str,
        ticker: str
    ) -> DataFrame:
        """pykrx 호환: get_market_cap의 별칭"""
        return self.get_market_cap(fromdate, todate, ticker)

    def get_market_fundamental_by_date(
        self,
        fromdate: str,
        todate: str,
        ticker: str
    ) -> DataFrame:
        """pykrx 호환: get_market_fundamental의 별칭"""
        return self.get_market_fundamental(fromdate, todate, ticker)

    def get_index_ohlcv_by_date(
        self,
        fromdate: str,
        todate: str,
        ticker: str,
        freq: str = "d"
    ) -> DataFrame:
        """pykrx 호환: get_index_ohlcv의 별칭"""
        return self.get_index_ohlcv(fromdate, todate, ticker, freq)

    # =========================================================================
    # 유틸리티
    # =========================================================================

    def get_nearest_business_day(self, target_date: Optional[str] = None) -> str:
        """
        가장 가까운 영업일 조회 (과거 방향으로 탐색)

        Args:
            target_date: 기준일 (YYYYMMDD), None이면 오늘

        Returns:
            영업일 (YYYYMMDD)

        Note:
            - 장 시작 전(09:00 이전)에는 전 영업일을 반환
            - 장 마감 후에는 당일을 반환
            - 전달된 날짜가 오늘인 경우에도 동일하게 적용
        """
        today = date.today()

        if target_date:
            dt = datetime.strptime(target_date, "%Y%m%d").date()
        else:
            dt = today

        kr_holidays = KR()

        # 오늘 날짜인 경우, 장 시작 전(09:00 이전)이면 전일 기준으로 탐색
        if dt == today:
            now = datetime.now()
            if now.hour < 9:
                # 오늘이 영업일이어도 아직 데이터가 없으므로 전일부터 탐색
                dt -= timedelta(days=1)

        # 최대 10일 전까지 탐색
        for _ in range(10):
            if self._is_market_day(dt, kr_holidays):
                return dt.strftime("%Y%m%d")
            dt -= timedelta(days=1)

        return dt.strftime("%Y%m%d")

    def _is_market_day(self, dt: date, kr_holidays) -> bool:
        """
        한국 주식 시장 영업일 여부 확인

        Args:
            dt: 확인할 날짜
            kr_holidays: 한국 공휴일 객체

        Returns:
            영업일이면 True
        """
        # 주말
        if dt.weekday() >= 5:
            return False

        # 공휴일
        if dt in kr_holidays:
            return False

        # 노동절 (5/1) - 증권시장 휴장
        if dt.month == 5 and dt.day == 1:
            return False

        # 연말 (12/31) - 증권시장 휴장
        if dt.month == 12 and dt.day == 31:
            return False

        # 연도별 특별 휴장일
        if dt.year == 2025:
            special_holidays = [
                (1, 27),   # 설날 연휴 임시공휴일
                (3, 3),    # 삼일절 대체공휴일
                (5, 6),    # 어린이날/부처님오신날 대체공휴일
                (6, 3),    # 대통령선거일
                (10, 8),   # 추석 대체공휴일
            ]
            if (dt.month, dt.day) in special_holidays:
                return False

        return True

    def is_market_day(self, target_date: Optional[str] = None) -> bool:
        """
        특정 날짜가 영업일인지 확인

        Args:
            target_date: 확인할 날짜 (YYYYMMDD), None이면 오늘

        Returns:
            영업일이면 True
        """
        if target_date:
            dt = datetime.strptime(target_date, "%Y%m%d").date()
        else:
            dt = date.today()

        return self._is_market_day(dt, KR())

    def close(self):
        """리소스 정리"""
        pass  # 현재는 특별히 정리할 것 없음

    def get_nearest_business_day_in_a_week(
        self,
        target_date: Optional[str] = None,
        prev: bool = True
    ) -> str:
        """
        pykrx 호환: 일주일 내 가장 가까운 영업일 조회

        Args:
            target_date: 기준일 (YYYYMMDD), None이면 오늘
            prev: True면 과거 방향, False면 미래 방향

        Returns:
            영업일 (YYYYMMDD)
        """
        # 현재는 과거 방향만 지원 (prev=True)
        return self.get_nearest_business_day(target_date)


# =============================================================================
# 모듈 레벨 편의 객체 (pykrx 호환용)
# =============================================================================

# 싱글톤 클라이언트 (lazy initialization)
_default_client: Optional[KRXDataClient] = None
_last_session_check_time: Optional[datetime] = None

# 세션 검증 생략 임계값 (이 시간 내에 검증했으면 재검증 생략)
FRESH_SESSION_THRESHOLD = timedelta(minutes=5)


def _get_client() -> KRXDataClient:
    """기본 클라이언트 반환 (lazy initialization)"""
    global _default_client
    if _default_client is None:
        _default_client = KRXDataClient()
    return _default_client


def ensure_session_valid() -> bool:
    """
    세션 유효성 확인 및 프리워밍 (orchestrator 시작 시 호출 권장)

    이 함수를 orchestrator 시작 시 한 번 호출하면:
    1. 세션이 없으면 로그인 (카카오 알림 1회)
    2. 세션이 있고 유효하면 그대로 사용
    3. 이후 FRESH_SESSION_THRESHOLD 동안 재검증 생략

    Returns:
        세션 유효 여부
    """
    global _last_session_check_time

    # 최근에 검증했으면 생략
    if _last_session_check_time and datetime.now() - _last_session_check_time < FRESH_SESSION_THRESHOLD:
        logger.debug(f"세션 최근 검증됨 ({_last_session_check_time}), 생략")
        return True

    try:
        client = _get_client()
        # 세션이 이미 로그인된 상태면 성공
        if client._auth_manager.is_logged_in:
            _last_session_check_time = datetime.now()
            logger.info("세션 프리워밍 완료 (기존 세션 사용)")
            return True

        # 세션 검증 및 필요시 로그인
        result = client._auth_manager.check_session()
        if result:
            _last_session_check_time = datetime.now()
            logger.info("세션 프리워밍 완료 (세션 검증/로그인)")
        return result
    except Exception as e:
        logger.error(f"세션 프리워밍 실패: {e}")
        return False


def is_session_fresh() -> bool:
    """세션이 최근에 검증되어 재검증이 불필요한지 확인"""
    global _last_session_check_time
    if _last_session_check_time is None:
        return False
    return datetime.now() - _last_session_check_time < FRESH_SESSION_THRESHOLD


def reset_session_freshness():
    """세션 freshness 리셋 (테스트용)"""
    global _last_session_check_time
    _last_session_check_time = None


# pykrx.stock.stock_api 호환 함수들
def get_market_ohlcv_by_date(fromdate: str, todate: str, ticker: str, adjusted: bool = True) -> DataFrame:
    """pykrx 호환: 개별종목 OHLCV"""
    return _get_client().get_market_ohlcv_by_date(fromdate, todate, ticker, adjusted)


def get_market_ohlcv_by_ticker(date: str, market: str = "ALL") -> DataFrame:
    """pykrx 호환: 특정일 전체 종목 OHLCV"""
    return _get_client().get_market_ohlcv_by_ticker(date, market)


def get_market_cap_by_ticker(date: str, market: str = "ALL") -> DataFrame:
    """pykrx 호환: 특정일 전체 종목 시가총액"""
    return _get_client().get_market_cap_by_ticker(date, market)


def get_market_cap_by_date(fromdate: str, todate: str, ticker: str) -> DataFrame:
    """pykrx 호환: 시가총액"""
    return _get_client().get_market_cap_by_date(fromdate, todate, ticker)


def get_market_fundamental_by_date(fromdate: str, todate: str, ticker: str) -> DataFrame:
    """pykrx 호환: 기본 지표"""
    return _get_client().get_market_fundamental_by_date(fromdate, todate, ticker)


def get_market_trading_volume_by_date(fromdate: str, todate: str, ticker: str, detail: bool = False) -> DataFrame:
    """pykrx 호환: 투자자별 거래량"""
    return _get_client().get_market_trading_volume_by_date(fromdate, todate, ticker, detail)


def get_market_trading_value_by_date(fromdate: str, todate: str, ticker: str, on: str = "순매수") -> DataFrame:
    """pykrx 호환: 일자별 거래대금"""
    return _get_client().get_market_trading_value_by_date(fromdate, todate, ticker, on)


def get_market_trading_volume_by_investor(fromdate: str, todate: str, ticker: str, detail: bool = False) -> DataFrame:
    """pykrx 호환: 투자자별 거래량"""
    return _get_client().get_market_trading_volume_by_date(fromdate, todate, ticker, detail)


def get_market_trading_value_by_investor(fromdate: str, todate: str, ticker: str, detail: bool = False) -> DataFrame:
    """pykrx 호환: 투자자별 거래대금"""
    return _get_client().get_market_trading_value_by_investor(fromdate, todate, ticker, detail)


def get_market_ticker_list(date: Optional[str] = None, market: str = "KOSPI") -> List[str]:
    """pykrx 호환: 티커 목록"""
    return _get_client().get_market_ticker_list(date, market)


def get_market_ticker_name(ticker: str) -> str:
    """pykrx 호환: 티커 이름"""
    client = _get_client()
    tickers = client.get_market_ticker_name(market="ALL")
    return tickers.get(ticker, "")


def get_index_ohlcv_by_date(fromdate: str, todate: str, ticker: str, freq: str = "d") -> DataFrame:
    """pykrx 호환: 지수 OHLCV"""
    return _get_client().get_index_ohlcv_by_date(fromdate, todate, ticker, freq)


def get_nearest_business_day_in_a_week(target_date: Optional[str] = None, prev: bool = True) -> str:
    """pykrx 호환: 일주일 내 가장 가까운 영업일"""
    return _get_client().get_nearest_business_day_in_a_week(target_date, prev)


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
