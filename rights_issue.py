import os
import json
import gspread
import pandas as pd
import requests
import zipfile
import io
import re
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# 1. GitHub Secrets 설정값
dart_key = os.environ['DART_API_KEY']
service_account_str = os.environ['GOOGLE_CREDENTIALS_JSON']
sheet_id = os.environ['GOOGLE_SHEET_ID']

# 2. 구글 시트 인증
creds = json.loads(service_account_str)
gc = gspread.service_account_from_dict(creds)
sh = gc.open_by_key(sheet_id)

# 새 구조 기준 컬럼 수 / 접수번호 위치
TOTAL_COLS = 21
NEW_RCEPT_IDX = 20   # 21번째 컬럼 (0-based)
OLD_RCEPT_IDX = 19   # 20번째 컬럼 (기존 구조 호환용)


# ==========================================================
# 공통 유틸
# ==========================================================
def normalize_text(text):
    s = str(text or "").replace("\xa0", " ")
    s = s.replace("\r", "\n")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{2,}", "\n", s)
    return s.strip()

def compact_text(text):
    return re.sub(r"\s+", "", str(text or ""))

def format_date_display(raw_date_str):
    if not raw_date_str:
        return '-'
    nums = re.findall(r'\d+', str(raw_date_str))
    if len(nums) >= 3:
        return f"{nums[0]}년 {nums[1].zfill(2)}월 {nums[2].zfill(2)}일"
    return '-'

def to_int(val):
    try:
        if pd.isna(val) or str(val).strip() == '':
            return 0
        return int(float(str(val).replace(',', '').strip()))
    except:
        return 0

def to_float(val):
    try:
        if pd.isna(val) or str(val).strip() == '':
            return 0.0
        return float(str(val).replace(',', '').replace('%', '').strip())
    except:
        return 0.0


# ==========================================================
# DART API
# ==========================================================
def fetch_dart_json(url, params):
    try:
        res = requests.get(url, params=params, timeout=20)
        if res.status_code == 200:
            data = res.json()
            if data.get('status') == '000' and 'list' in data:
                return pd.DataFrame(data['list'])
    except Exception as e:
        print(f"JSON API 에러: {e}")
    return pd.DataFrame()

def fetch_dart_list_all(url, params):
    frames = []
    page_no = 1
    total_page = 1

    while page_no <= total_page:
        page_params = params.copy()
        page_params['page_no'] = page_no
        page_params['page_count'] = '100'

        try:
            res = requests.get(url, params=page_params, timeout=20)
            if res.status_code != 200:
                print(f"list.json HTTP 에러: {res.status_code}")
                break

            data = res.json()

            if data.get('status') == '013':
                break

            if data.get('status') != '000':
                print(f"list.json API 에러: {data.get('status')} / {data.get('message')}")
                break

            total_page = int(data.get('total_page', 1))

            if data.get('list'):
                frames.append(pd.DataFrame(data['list']))

            page_no += 1

        except Exception as e:
            print(f"list.json 페이지 수집 에러: {e}")
            break

    if frames:
        return pd.concat(frames, ignore_index=True)

    return pd.DataFrame()


# ==========================================================
# XML 추출 보조 함수
# ==========================================================
def find_date_from_lines(lines, include_keywords, exclude_keywords=None):
    exclude_keywords = exclude_keywords or []
    include_compact = [compact_text(x) for x in include_keywords]
    exclude_compact = [compact_text(x) for x in exclude_keywords]

    for i, line in enumerate(lines):
        c_line = compact_text(line)

        if all(k in c_line for k in include_compact) and not any(k in c_line for k in exclude_compact):
            m = re.search(r'(\d{4}[년\.\-/\s]+\d{1,2}[월\.\-/\s]+\d{1,2}일?)', line)
            if m:
                return format_date_display(m.group(1))

            if i + 1 < len(lines):
                nxt = lines[i + 1]
                m2 = re.search(r'(\d{4}[년\.\-/\s]+\d{1,2}[월\.\-/\s]+\d{1,2}일?)', nxt)
                if m2:
                    return format_date_display(m2.group(1))

    return '-'

def find_value_near_label(text, label_patterns, value_pattern, window=160):
    for label_pat in label_patterns:
        for m in re.finditer(label_pat, text, flags=re.IGNORECASE):
            snippet = text[m.end():m.end() + window]
            found = re.search(value_pattern, snippet, flags=re.IGNORECASE)
            if found:
                return found.group(1).strip()
    return None

def find_line_value(lines, include_keywords, exclude_keywords=None):
    exclude_keywords = exclude_keywords or []
    include_compact = [compact_text(x) for x in include_keywords]
    exclude_compact = [compact_text(x) for x in exclude_keywords]

    for i, line in enumerate(lines):
        c_line = compact_text(line)

        if all(k in c_line for k in include_compact) and not any(k in c_line for k in exclude_compact):
            parts = re.split(r'[:：]', line, maxsplit=1)
            if len(parts) == 2 and parts[1].strip():
                return parts[1].strip()

            if i + 1 < len(lines):
                nxt = lines[i + 1].strip()
                if nxt:
                    return nxt

    return None


# ==========================================================
# XML 원문 추출
# ==========================================================
def extract_xml_details(api_key, rcept_no):
    url = "https://opendart.fss.or.kr/api/document.xml"
    params = {'crtfc_key': api_key, 'rcept_no': rcept_no}

    extracted = {
        'first_board_date': '-',
        'board_date': '-',
        'issue_price': '-',
        'base_price': '-',
        'discount': '-',
        'pay_date': '-',
        'div_date': '-',
        'list_date': '-',
        'investor': '원문참조'
    }

    try:
        res = requests.get(url, params=params, stream=True, timeout=30)
        if res.status_code != 200:
            return extracted

        with zipfile.ZipFile(io.BytesIO(res.content)) as z:
            xml_files = [name for name in z.namelist() if name.endswith('.xml')]
            if not xml_files:
                return extracted

            with z.open(xml_files[0]) as f:
                xml_content = f.read().decode('utf-8', errors='ignore')

        soup = BeautifulSoup(xml_content, 'html.parser')
        raw_text = soup.get_text(separator='\n', strip=True)
        text = normalize_text(raw_text)
        lines = [normalize_text(x) for x in raw_text.split('\n') if normalize_text(x)]

        # --------------------------------------------------
        # 날짜
        # --------------------------------------------------
        extracted['first_board_date'] = find_date_from_lines(
            lines, include_keywords=['최초', '이사회결의일']
        )

        extracted['board_date'] = find_date_from_lines(
            lines, include_keywords=['이사회결의일'], exclude_keywords=['최초']
        )

        if extracted['board_date'] == '-':
            extracted['board_date'] = extracted['first_board_date']

        extracted['pay_date'] = find_date_from_lines(lines, include_keywords=['납입일'])
        extracted['div_date'] = find_date_from_lines(lines, include_keywords=['배당기산일'])
        extracted['list_date'] = find_date_from_lines(lines, include_keywords=['상장예정일'])

        # --------------------------------------------------
        # 확정발행가(원)
        # 우선순위: 확정발행가액 > 확정발행가 > 1주당 발행가액 > 발행가액
        # --------------------------------------------------
        issue_price = find_value_near_label(
            text,
            [
                r'확정\s*발행가액',
                r'확정\s*발행가',
                r'1주당\s*발행가액',
                r'발행가액'
            ],
            r'([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?)',
            window=120
        )
        if issue_price:
            extracted['issue_price'] = issue_price

        # --------------------------------------------------
        # 기준주가
        # --------------------------------------------------
        base_price = find_value_near_label(
            text,
            [
                r'산정\s*기준주가',
                r'기준주가'
            ],
            r'([0-9]{1,3}(?:,[0-9]{3})*(?:\.\d+)?)',
            window=120
        )
        if base_price:
            extracted['base_price'] = base_price

        # --------------------------------------------------
        # 할인율 / 할인률 / 할증률
        # --------------------------------------------------
        discount = find_value_near_label(
            text,
            [
                r'할인율',
                r'할인률',
                r'할증률',
                r'할증율'
            ],
            r'([+\-]?\d+(?:\.\d+)?)',
            window=80
        )
        if discount:
            extracted['discount'] = discount + "%"

        # --------------------------------------------------
        # 투자자 / 배정대상자
        # --------------------------------------------------
        investor = (
            find_line_value(lines, ['제3자배정', '대상자']) or
            find_line_value(lines, ['배정대상자']) or
            find_line_value(lines, ['제3자배정'])
        )

        if investor:
            investor = re.sub(r'\s+', ' ', investor).strip()
            if len(investor) <= 150:
                extracted['investor'] = investor
        else:
            if "제3자배정" in compact_text(text):
                extracted['investor'] = "제3자배정 (원문참조)"

    except Exception as e:
        print(f"문서 XML 에러 ({rcept_no}): {e}")

    return extracted


# ==========================================================
# 메인
# ==========================================================
def get_and_update_yusang():
    start_date = '20260101'
    end_date = '20260131'

    print(f"{start_date} ~ {end_date} 유상증자 공시 탐색 중...")

    list_url = "https://opendart.fss.or.kr/api/list.json"
    list_params = {
        'crtfc_key': dart_key,
        'bgn_de': start_date,
        'end_de': end_date,
        'pblntf_ty': 'B',
        'pblntf_detail_ty': 'B001'
    }

    all_filings = fetch_dart_list_all(list_url, list_params)

    if all_filings.empty:
        print("최근 지정 기간 내 주요사항보고서가 없습니다.")
        return

    df_filtered = all_filings[
        all_filings['report_nm'].str.contains('유상증자결정', na=False)
    ].copy()

    if df_filtered.empty:
        print("ℹ️ 유상증자 공시가 없습니다.")
        return

    corp_codes = df_filtered['corp_code'].astype(str).unique()
    detail_dfs = []

    # piicDecsn은 최초접수일 기준 이슈가 있을 수 있어 조회 시작일을 넉넉하게 확장
    detail_start_date = (
        datetime.strptime(start_date, "%Y%m%d") - timedelta(days=180)
    ).strftime("%Y%m%d")

    for code in corp_codes:
        detail_params = {
            'crtfc_key': dart_key,
            'corp_code': code,
            'bgn_de': detail_start_date,
            'end_de': end_date
        }
        df_detail = fetch_dart_json(
            'https://opendart.fss.or.kr/api/piicDecsn.json',
            detail_params
        )
        if not df_detail.empty:
            detail_dfs.append(df_detail)

    if not detail_dfs:
        print("ℹ️ 상세 데이터를 불러올 수 없습니다.")
        return

    df_combined = pd.concat(detail_dfs, ignore_index=True)

    # report_nm 병합
    df_filtered['rcept_no'] = df_filtered['rcept_no'].astype(str)
    df_combined['rcept_no'] = df_combined['rcept_no'].astype(str)

    df_merged = pd.merge(
        df_combined,
        df_filtered[['rcept_no', 'report_nm']],
        on='rcept_no',
        how='inner'
    )

    worksheet = sh.worksheet('D_유상증자')

    # 기존 시트 전체 읽기
    all_sheet_data = worksheet.get_all_values()
    existing_data_dict = {}

    # 기존 20컬럼 구조 / 신규 21컬럼 구조 둘 다 호환
    for idx, row_data in enumerate(all_sheet_data):
        rcept_val = ''
        if len(row_data) > NEW_RCEPT_IDX and str(row_data[NEW_RCEPT_IDX]).strip():
            rcept_val = str(row_data[NEW_RCEPT_IDX]).strip()
        elif len(row_data) > OLD_RCEPT_IDX and str(row_data[OLD_RCEPT_IDX]).strip():
            rcept_val = str(row_data[OLD_RCEPT_IDX]).strip()

        if rcept_val:
            existing_data_dict[rcept_val] = {
                'row_idx': idx + 1,
                'data': [str(x).strip() for x in row_data]
            }

    data_to_add = []
    cls_map = {'Y': '유가', 'K': '코스닥', 'N': '코넥스', 'E': '기타'}

    for _, row in df_merged.iterrows():
        rcept_no = str(row.get('rcept_no', '')).strip()
        corp_name = str(row.get('corp_name', '')).strip()
        report_nm = str(row.get('report_nm', '')).strip()

        xml_data = extract_xml_details(dart_key, rcept_no)

        # 1. 상장시장
        market = cls_map.get(str(row.get('corp_cls', '')).strip(), '기타')
        method = str(row.get('ic_mthn', '')).strip()

        # 2. 신규발행주식수 / 증자전 주식수
        # -> JSON 값을 최우선으로 고정 사용
        ostk = to_int(row.get('nstk_ostk_cnt'))
        estk = to_int(row.get('nstk_estk_cnt'))
        new_shares = ostk + estk

        old_ostk = to_int(row.get('bfic_tisstk_ostk'))
        old_estk = to_int(row.get('bfic_tisstk_estk'))
        old_shares = old_ostk + old_estk

        if ostk > 0 and estk > 0:
            product = "보통주+기타주"
        elif ostk > 0:
            product = "보통주"
        elif estk > 0:
            product = "기타주"
        else:
            product = "-"

        new_shares_str = f"{new_shares:,}" if new_shares > 0 else "-"
        old_shares_str = f"{old_shares:,}" if old_shares > 0 else "-"

        # 3. 증자비율
        ratio = f"{(new_shares / old_shares * 100):.2f}%" if old_shares > 0 else "-"

        # 4. 확정발행금액(억원)
        # 우선순위:
        # 1) 신규발행주식수 * 확정발행가
        # 2) 안 되면 자금조달 목적 합계 fallback
        issue_price_num = to_float(xml_data['issue_price'])

        if new_shares > 0 and issue_price_num > 0:
            total_amt = new_shares * issue_price_num
            total_amt_uk = f"{(total_amt / 100000000):,.2f}"
        else:
            fclt = to_int(row.get('fdpp_fclt'))
            bsninh = to_int(row.get('fdpp_bsninh'))
            op = to_int(row.get('fdpp_op'))
            dtrp = to_int(row.get('fdpp_dtrp'))
            ocsa = to_int(row.get('fdpp_ocsa'))
            etc = to_int(row.get('fdpp_etc'))

            total_amt = fclt + bsninh + op + dtrp + ocsa + etc
            total_amt_uk = f"{(total_amt / 100000000):,.2f}" if total_amt > 0 else "-"

        # 5. 자금용도
        fclt = to_int(row.get('fdpp_fclt'))
        bsninh = to_int(row.get('fdpp_bsninh'))
        op = to_int(row.get('fdpp_op'))
        dtrp = to_int(row.get('fdpp_dtrp'))
        ocsa = to_int(row.get('fdpp_ocsa'))
        etc = to_int(row.get('fdpp_etc'))

        purposes = []
        if fclt > 0:
            purposes.append("시설")
        if bsninh > 0:
            purposes.append("영업양수")
        if op > 0:
            purposes.append("운영")
        if dtrp > 0:
            purposes.append("채무상환")
        if ocsa > 0:
            purposes.append("타법인증권")
        if etc > 0:
            purposes.append("기타")
        purpose_str = ", ".join(purposes)

        link = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"

        print(
            f"[DEBUG] {corp_name} / {rcept_no} | "
            f"신규발행주식수={new_shares_str}, "
            f"확정발행가={xml_data['issue_price']}, "
            f"기준주가={xml_data['base_price']}, "
            f"확정발행금액(억원)={total_amt_uk}, "
            f"증자전주식수={old_shares_str}"
        )

        new_row = [
            corp_name,                         # 1 회사명
            report_nm,                         # 2 보고서명
            market,                            # 3 상장시장
            xml_data['first_board_date'],      # 4 최초 이사회결의일
            method,                            # 5 증자방식
            product,                           # 6 발행주식종류
            new_shares_str,                    # 7 신규발행주식수
            xml_data['issue_price'],           # 8 확정발행가
            xml_data['base_price'],            # 9 기준주가
            total_amt_uk,                      # 10 확정발행금액(억원)
            xml_data['discount'],              # 11 할인율
            old_shares_str,                    # 12 증자전 주식수
            ratio,                             # 13 증자비율
            xml_data['pay_date'],              # 14 납입일
            xml_data['div_date'],              # 15 배당기산일
            xml_data['list_date'],             # 16 상장예정일
            xml_data['board_date'],            # 17 이사회결의일
            purpose_str,                       # 18 자금용도
            xml_data['investor'],              # 19 투자자
            link,                              # 20 링크
            rcept_no                           # 21 접수번호
        ]

        new_row_str = [str(x).strip() for x in new_row]

        if rcept_no in existing_data_dict:
            existing_row_str = existing_data_dict[rcept_no]['data']

            existing_row_str += [''] * (len(new_row_str) - len(existing_row_str))
            existing_row_str = existing_row_str[:len(new_row_str)]

            if new_row_str != existing_row_str:
                row_idx = existing_data_dict[rcept_no]['row_idx']
                try:
                    worksheet.update(range_name=f'A{row_idx}:U{row_idx}', values=[new_row])
                except TypeError:
                    worksheet.update(f'A{row_idx}:U{row_idx}', [new_row])
                print(f"🔄 {corp_name}: 데이터 변경 감지! 최신 내용으로 자동 덮어쓰기 완료 (행: {row_idx})")
            else:
                print(f"⏩ {corp_name}: 변경사항 없음 (패스)")
        else:
            print(f"🆕 {corp_name}: 신규 공시 발견! 추가 대기 중...")
            data_to_add.append(new_row)

    if data_to_add:
        worksheet.append_rows(data_to_add)
        print(f"✅ 유상증자: 신규 데이터 {len(data_to_add)}건 일괄 추가 완료!")
    else:
        print("✅ 유상증자: 새로 추가할 공시는 없으며 데이터 최신화 점검을 마쳤습니다.")


if __name__ == "__main__":
    get_and_update_yusang()
