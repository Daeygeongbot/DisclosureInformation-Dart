import os
import json
import gspread
import pandas as pd
import requests
import zipfile
import io
import re
import time
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

# 시트 구조
TOTAL_COLS = 21
NEW_RCEPT_IDX = 20   # 21번째 컬럼 (0-based)
OLD_RCEPT_IDX = 19   # 20번째 컬럼 (기존 구조 호환용)


# =========================================================
# 공통 유틸
# =========================================================
def first_nonempty(*vals):
    for v in vals:
        if v is None:
            continue
        s = str(v).strip()
        if s and s != '-':
            return s
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
        if val is None:
            return None
        s = str(val).replace(',', '').replace('%', '').strip()
        if s == '':
            return None
        return float(s)
    except:
        return None


def format_int(val):
    try:
        n = int(round(float(val)))
        return f"{n:,}"
    except:
        return '-'


def format_rate(val):
    if val is None:
        return '-'
    try:
        v = float(val)
        s = f"{v:.2f}".rstrip('0').rstrip('.')
        return f"{s}%"
    except:
        return '-'


def fix_date(raw_date_str):
    if not raw_date_str:
        return '-'
    nums = re.findall(r'\d+', str(raw_date_str))
    if len(nums) >= 3:
        return f"{nums[0]}년 {nums[1].zfill(2)}월 {nums[2].zfill(2)}일"
    return '-'


def unique_keep_order(seq):
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


# =========================================================
# JSON API
# =========================================================
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


# =========================================================
# XML 파싱
# =========================================================
def get_xml_clean_text(api_key, rcept_no):
    url = "https://opendart.fss.or.kr/api/document.xml"
    params = {'crtfc_key': api_key, 'rcept_no': rcept_no}

    try:
        res = requests.get(url, params=params, stream=True, timeout=30)
        if res.status_code != 200:
            return ''

        with zipfile.ZipFile(io.BytesIO(res.content)) as z:
            xml_files = [name for name in z.namelist() if name.endswith('.xml')]
            if not xml_files:
                return ''

            with z.open(xml_files[0]) as f:
                xml_content = f.read().decode('utf-8', errors='ignore')
                soup = BeautifulSoup(xml_content, 'html.parser')

                for tag in soup.find_all(['td', 'th', 'p', 'div', 'li', 'span']):
                    tag.append(' ')

                raw_text = soup.get_text(separator=' ', strip=True)
                clean_text = raw_text.replace('\xa0', ' ')
                clean_text = re.sub(r'\s+', ' ', clean_text).strip()
                return clean_text

    except Exception as e:
        print(f"문서 XML 에러 ({rcept_no}): {e}")
        return ''


def extract_date_by_labels(text, labels):
    if not text:
        return '-'

    for label in labels:
        pattern = rf'{re.escape(label)}[^\d]{{0,20}}(\d{{4}}[\-\.년\s]+\d{{1,2}}[\-\.월\s]+\d{{1,2}})'
        m = re.search(pattern, text)
        if m:
            return fix_date(m.group(1))

    return '-'


def extract_discount_rate(text):
    if not text:
        return None

    candidates = []

    # 예: 할인율 10%, 할증률 5.5%, 할인율 -10.0%
    pattern = r'(할인|할증)\s*[율률][^\d+\-]{0,20}([+\-]?\d+(?:\.\d+)?)\s*%'
    for m in re.finditer(pattern, text):
        kind = m.group(1)
        num = to_float(m.group(2))
        if num is None:
            continue

        if kind == '할인' and num > 0:
            num = -num
        elif kind == '할증' and num < 0:
            num = abs(num)

        if -100 < num < 100:
            candidates.append(num)

    # 퍼센트 기호 없는 경우 fallback
    if not candidates:
        pattern2 = r'(할인|할증)\s*[율률][^\d+\-]{0,20}([+\-]?\d+(?:\.\d+)?)'
        for m in re.finditer(pattern2, text):
            kind = m.group(1)
            num = to_float(m.group(2))
            if num is None:
                continue

            if kind == '할인' and num > 0:
                num = -num
            elif kind == '할증' and num < 0:
                num = abs(num)

            if -100 < num < 100:
                candidates.append(num)

    candidates = unique_keep_order(candidates)
    return candidates[0] if candidates else None


def extract_number_candidates_near_labels(text, labels, window=120):
    if not text:
        return []

    candidates = []

    for label in labels:
        for m in re.finditer(re.escape(label), text):
            snippet = text[m.end(): m.end() + window]
            nums = re.findall(r'(?<!\d)(\d{1,3}(?:,\d{3})+|\d+)(?!\d)', snippet)
            for num in nums:
                val = to_int(num)
                if val > 0:
                    candidates.append(val)

    return unique_keep_order(candidates)


def pick_best_price_candidate(candidates, expected=None, min_value=1, max_value=50000000):
    valid = [x for x in candidates if min_value <= x <= max_value]
    if not valid:
        return None

    if expected and expected > 0:
        valid.sort(key=lambda x: (abs(x - expected), x))
        return valid[0]

    return valid[0]


def extract_issue_price_from_text(text, expected=None):
    labels = [
        '확정 발행가액',
        '확정발행가액',
        '1주당 발행가액',
        '발행가액'
    ]
    candidates = extract_number_candidates_near_labels(text, labels, window=100)
    return pick_best_price_candidate(candidates, expected=expected, min_value=1, max_value=50000000)


def extract_base_price_from_text(text, expected=None):
    if not text:
        return None

    labels = [
        '산정 기준주가',
        '산정기준주가',
        '기준주가'
    ]

    candidates = []

    # 1) 가장 우선: "기준주가 12,345원" 형태만 먼저 찾음
    for label in labels:
        pattern1 = rf'{re.escape(label)}\s*[:：]?\s*(\d{{1,3}}(?:,\d{{3}})+|\d+)\s*원'
        for m in re.finditer(pattern1, text):
            val = to_int(m.group(1))
            if 100 <= val <= 50000000:
                candidates.append(val)

    # 2) 보조: "기준주가 12345" 형태 허용
    # 단, 바로 뒤가 %, 년, 월, 일, 차, 회, 번 이면 제외
    if not candidates:
        for label in labels:
            pattern2 = rf'{re.escape(label)}\s*[:：]?\s*(\d{{1,3}}(?:,\d{{3}})+|\d+)'
            for m in re.finditer(pattern2, text):
                val_str = m.group(1)
                tail = text[m.end():m.end() + 3]

                if any(tail.startswith(x) for x in ['%', '년', '월', '일', '차', '회', '번']):
                    continue

                val = to_int(val_str)
                if 100 <= val <= 50000000:
                    candidates.append(val)

    # 3) 마지막 fallback:
    # "기준주가" 바로 뒤의 아주 짧은 구간(최대 20자)만 확인
    # -> "7. 기준주가", "25. 청약..." 같은 항목 번호 오탐 방지
    if not candidates:
        for label in labels:
            for m in re.finditer(re.escape(label), text):
                snippet = text[m.end(): m.end() + 20]
                snippet = re.sub(r'^[\s:：\-=·•\)\]\}]*', '', snippet)

                m2 = re.match(r'(\d{1,3}(?:,\d{3})+|\d+)', snippet)
                if not m2:
                    continue

                val_str = m2.group(1)
                tail = snippet[m2.end():m2.end() + 3]

                if any(tail.startswith(x) for x in ['%', '년', '월', '일', '차', '회', '번']):
                    continue

                val = to_int(val_str)
                if 100 <= val <= 50000000:
                    candidates.append(val)

    candidates = unique_keep_order(candidates)
    return pick_best_price_candidate(candidates, expected=expected, min_value=100, max_value=50000000)


def extract_investor_from_text(text):
    if not text:
        return '원문참조'

    if '제3자배정' in text:
        return '제3자배정 (원문참조)'

    m = re.search(
        r'배정\s*대상자[^\w가-힣]{0,10}(주식회사\s*[^\s,\.]+|[^\s,\.]+(?:투자조합|펀드|유한회사|주식회사))',
        text
    )
    if m:
        return m.group(1).strip()

    return '원문참조'


def extract_xml_details(api_key, rcept_no):
    text = get_xml_clean_text(api_key, rcept_no)

    return {
        'clean_text': text,
        'board_date': first_nonempty(
            extract_date_by_labels(text, ['최초 이사회결의일', '최초이사회결의일']),
            extract_date_by_labels(text, ['이사회결의일'])
        ),
        'pay_date': extract_date_by_labels(text, ['납입일']),
        'div_date': extract_date_by_labels(text, ['배당기산일']),
        'list_date': extract_date_by_labels(text, ['상장 예정일', '상장예정일']),
        'investor': extract_investor_from_text(text)
    }


# =========================================================
# 행 데이터 생성
# =========================================================
def make_row_data(row, xml_data, cls_map):
    rcept_no = str(row.get('rcept_no', ''))
    corp_name = row.get('corp_name', '')
    report_nm = row.get('report_nm', '')

    market = cls_map.get(row.get('corp_cls', ''), '기타')
    method = str(row.get('ic_mthn', '')).strip() or '-'

    # 신규발행주식수
    ostk = to_int(row.get('nstk_ostk_cnt'))
    estk = to_int(row.get('nstk_estk_cnt'))
    new_shares = ostk + estk

    if ostk > 0 and estk > 0:
        product = "보통주+기타주"
    elif ostk > 0:
        product = "보통주"
    elif estk > 0:
        product = "기타주"
    else:
        product = "-"

    # 증자전 주식수
    old_ostk = to_int(row.get('bfic_tisstk_ostk'))
    old_estk = to_int(row.get('bfic_tisstk_estk'))
    old_shares = old_ostk + old_estk

    # 자금조달금액
    fclt = to_int(row.get('fdpp_fclt'))
    bsninh = to_int(row.get('fdpp_bsninh'))
    op = to_int(row.get('fdpp_op'))
    dtrp = to_int(row.get('fdpp_dtrp'))
    ocsa = to_int(row.get('fdpp_ocsa'))
    etc = to_int(row.get('fdpp_etc'))
    total_amt = fclt + bsninh + op + dtrp + ocsa + etc

    # 자금용도
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
    purpose_str = ", ".join(purposes) if purposes else "-"

    # 확정발행가(원) : 총액 / 신규발행주식수 우선
    issue_price_math = None
    if total_amt > 0 and new_shares > 0:
        issue_price_math = int(round(total_amt / new_shares))

    issue_price_xml = extract_issue_price_from_text(xml_data['clean_text'], expected=issue_price_math)
    issue_price_int = issue_price_math if issue_price_math and issue_price_math > 0 else issue_price_xml

    # 할인율
    discount_val = extract_discount_rate(xml_data['clean_text'])

    # 기준주가 : XML 우선, 없으면 issue/discount 역산
    expected_base = None
    if issue_price_int and discount_val is not None and (1 + discount_val / 100) != 0:
        expected_base = issue_price_int / (1 + discount_val / 100)

    base_price_xml = extract_base_price_from_text(xml_data['clean_text'], expected=expected_base)

    base_price_int = None
    if base_price_xml and base_price_xml > 0:
        base_price_int = base_price_xml
    elif issue_price_int and discount_val is not None and (1 + discount_val / 100) != 0:
        base_price_int = int(round(issue_price_int / (1 + discount_val / 100)))

    # 할인율 재보정
    if issue_price_int and base_price_int and base_price_int > 0:
        derived_discount = round((issue_price_int / base_price_int - 1) * 100, 2)
        if discount_val is None or abs(discount_val - derived_discount) > 0.3:
            discount_val = derived_discount

    # 증자비율
    ratio = f"{(new_shares / old_shares * 100):.2f}%" if old_shares > 0 else "-"

    # 문자열 포맷
    new_shares_str = format_int(new_shares) if new_shares > 0 else "0"
    old_shares_str = format_int(old_shares) if old_shares > 0 else "0"
    issue_price_str = format_int(issue_price_int) if issue_price_int else "-"
    base_price_str = format_int(base_price_int) if base_price_int else "-"
    total_amt_uk = f"{(total_amt / 100000000):,.2f}" if total_amt > 0 else "0.00"
    discount_str = format_rate(discount_val)

    # 간단 검산 로그
    if issue_price_int and base_price_int and issue_price_int > 0 and base_price_int > 0:
        if issue_price_int > 50000000 or base_price_int > 50000000:
            print(f" ⚠️ {corp_name} ({rcept_no}) 발행가/기준주가 비정상 추정치 감지")

    link = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"

    return [
        corp_name,                      # 1 회사명
        report_nm,                      # 2 보고서명
        market,                         # 3 상장시장
        xml_data['board_date'],         # 4 최초 이사회결의일
        method,                         # 5 증자방식
        product,                        # 6 발행주식종류
        new_shares_str,                 # 7 신규발행주식수
        issue_price_str,                # 8 확정발행가(원)
        base_price_str,                 # 9 기준주가
        total_amt_uk,                   # 10 확정발행금액(억원)
        discount_str,                   # 11 할인(할증률)
        old_shares_str,                 # 12 증자전 주식수
        ratio,                          # 13 증자비율
        xml_data['pay_date'],           # 14 납입일
        xml_data['div_date'],           # 15 배당기산일
        xml_data['list_date'],          # 16 상장예정일
        xml_data['board_date'],         # 17 이사회결의일
        purpose_str,                    # 18 자금용도
        xml_data['investor'],           # 19 투자자
        link,                           # 20 링크
        rcept_no                        # 21 접수번호
    ]


# =========================================================
# 시트 기존 접수번호 맵
# =========================================================
def build_rcept_row_map(all_sheet_data):
    rcept_row_map = {}

    for idx, row in enumerate(all_sheet_data):
        # 보통 1행은 헤더이므로 skip
        if idx == 0:
            continue

        rcept_val = ''
        if len(row) > NEW_RCEPT_IDX and str(row[NEW_RCEPT_IDX]).strip():
            rcept_val = str(row[NEW_RCEPT_IDX]).strip()
        elif len(row) > OLD_RCEPT_IDX and str(row[OLD_RCEPT_IDX]).strip():
            rcept_val = str(row[OLD_RCEPT_IDX]).strip()

        if rcept_val:
            rcept_row_map[rcept_val] = idx + 1

    return rcept_row_map

# =========================================================
# 메인
# =========================================================
def get_and_update_yusang(start_date, end_date):
    print(f"{start_date} ~ {end_date} 유상증자 공시 탐색 중...")

    # 1) list.json 전체 조회
    list_url = "https://opendart.fss.or.kr/api/list.json"
    list_params = {
        'crtfc_key': dart_key,
        'bgn_de': start_date,
        'end_de': end_date
    }
    all_filings = fetch_dart_list_all(list_url, list_params)

    if all_filings.empty:
        print("최근 지정 기간 내 공시가 없습니다.")
        return

    # 2) report_nm으로 유상증자결정만 필터
    df_filtered = all_filings[
        all_filings['report_nm'].astype(str).str.contains('유상증자결정', na=False)
    ].copy()

    if df_filtered.empty:
        print("ℹ️ 유상증자 공시가 없습니다.")
        return

    df_filtered['rcept_no'] = df_filtered['rcept_no'].astype(str)
    df_filtered = df_filtered.drop_duplicates(subset=['rcept_no'])
    df_report_map = df_filtered[['rcept_no', 'report_nm']].drop_duplicates(subset=['rcept_no'])

    # 3) piicDecsn.json은 최초접수일 기준 이슈 있으므로 넉넉하게 180일 확장
    corp_codes = df_filtered['corp_code'].astype(str).unique()
    detail_dfs = []

    detail_start_date = (datetime.strptime(start_date, "%Y%m%d") - timedelta(days=180)).strftime("%Y%m%d")

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
    df_combined['rcept_no'] = df_combined['rcept_no'].astype(str)

    target_rcept_nos = df_filtered['rcept_no'].unique()
    df_merged = df_combined[df_combined['rcept_no'].isin(target_rcept_nos)].copy()

    # report_nm 붙이기
    df_merged = df_merged.merge(df_report_map, on='rcept_no', how='left')
    df_merged = df_merged.drop_duplicates(subset=['rcept_no'], keep='last')

    if df_merged.empty:
        print("ℹ️ 최종 병합된 유상증자 상세 데이터가 없습니다.")
        return

    worksheet = sh.worksheet('D_유상증자')
    cls_map = {'Y': '유가', 'K': '코스닥', 'N': '코넥스', 'E': '기타'}

    # 기존 시트 데이터
    all_sheet_data = worksheet.get_all_values()
    rcept_row_map = build_rcept_row_map(all_sheet_data)
    existing_rcept_nos = list(rcept_row_map.keys())

    # =====================================================
    # 신규 데이터 추가
    # =====================================================
    new_data_df = df_merged[~df_merged['rcept_no'].astype(str).isin(existing_rcept_nos)]
    data_to_add = []

    for _, row in new_data_df.iterrows():
        rcept_no = str(row.get('rcept_no', ''))
        corp_name = row.get('corp_name', '')
        print(f" -> [신규] {corp_name} 데이터 포매팅 중...")

        xml_data = extract_xml_details(dart_key, rcept_no)
        new_row = make_row_data(row, xml_data, cls_map)
        data_to_add.append(new_row)

    if data_to_add:
        worksheet.append_rows(data_to_add)
        print(f"✅ 유상증자: 신규 데이터 {len(data_to_add)}건 추가 완료!")

        all_sheet_data = worksheet.get_all_values()
        rcept_row_map = build_rcept_row_map(all_sheet_data)
        existing_rcept_nos = list(rcept_row_map.keys())

    # =====================================================
    # 기존 데이터 재검사 및 덮어쓰기
    # =====================================================
    existing_data_df = df_merged[df_merged['rcept_no'].astype(str).isin(existing_rcept_nos)]
    update_count = 0

    for _, row in existing_data_df.iterrows():
        rcept_no = str(row.get('rcept_no', ''))
        row_idx = rcept_row_map.get(rcept_no)
        if not row_idx:
            continue

        if row_idx - 1 >= len(all_sheet_data):
            continue

        sheet_row = all_sheet_data[row_idx - 1]
        sheet_row_padded = sheet_row + [''] * (TOTAL_COLS - len(sheet_row))
        sheet_row_padded = [str(x).strip() for x in sheet_row_padded[:TOTAL_COLS]]

        xml_data = extract_xml_details(dart_key, rcept_no)
        new_row = make_row_data(row, xml_data, cls_map)
        new_row_str = [str(x).strip() for x in new_row]

        if sheet_row_padded != new_row_str:
            corp_name = row.get('corp_name', '')
            print(f" 🔄 [업데이트] {corp_name} 값이 변경/확정되었습니다. 시트를 덮어씁니다.")
            worksheet.update(range_name=f'A{row_idx}:U{row_idx}', values=[new_row])
            update_count += 1
            time.sleep(1)

    if update_count > 0:
        print(f"✅ 유상증자: 기존 데이터 {update_count}건 자동 업데이트 완료!")
    else:
        print("✅ 유상증자: 새로 추가할 공시는 없으며 데이터 최신화 점검을 마쳤습니다.")


if __name__ == "__main__":
    get_and_update_yusang('20260101', '20260131')
