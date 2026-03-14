#유상증자 전용 코드
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

# --- [JSON 파싱] ---
def fetch_dart_json(url, params):
    try:
        res = requests.get(url, params=params)
        if res.status_code == 200:
            data = res.json()
            if data.get('status') == '000' and 'list' in data:
                return pd.DataFrame(data['list'])
    except Exception as e:
        print(f"JSON API 에러: {e}")
    return pd.DataFrame()

# --- [XML 원문 족집게 파싱 (정규식 초정밀 업그레이드)] ---
def extract_xml_details(api_key, rcept_no):
    url = "https://opendart.fss.or.kr/api/document.xml"
    params = {'crtfc_key': api_key, 'rcept_no': rcept_no}
    
    extracted = {
        'board_date': '-', 'issue_price': '-', 'base_price': '-', 'discount': '-',
        'pay_date': '-', 'div_date': '-', 'list_date': '-', 'investor': '원문참조'
    }
    
    try:
        res = requests.get(url, params=params, stream=True)
        if res.status_code == 200:
            with zipfile.ZipFile(io.BytesIO(res.content)) as z:
                xml_filename = [name for name in z.namelist() if name.endswith('.xml')][0]
                with z.open(xml_filename) as f:
                    xml_content = f.read().decode('utf-8')
                    soup = BeautifulSoup(xml_content, 'html.parser')
                    raw_text = soup.get_text(separator=' ', strip=True)
                    
                    def fix_date(raw_date_str):
                        if not raw_date_str:
                            return '-'
                        nums = re.findall(r'\d+', raw_date_str)
                        if len(nums) >= 3:
                            return f"{nums[0]}년 {nums[1].zfill(2)}월 {nums[2].zfill(2)}일"
                        return raw_date_str + "일"
                    
                    # 1. 확정발행가 추출
                    issue = re.search(r'발행가액[^\d]*([0-9]{1,3}(?:,[0-9]{3})*)', raw_text)
                    if issue:
                        extracted['issue_price'] = issue.group(1).strip()
                    
                    # 2. 기준주가 추출
                    base = re.search(r'기준주가[^\d]*([0-9]{1,3}(?:,[0-9]{3})*)', raw_text)
                    if base:
                        extracted['base_price'] = base.group(1).strip()
                    
                    # 3. 할인/할증률 추출
                    disc = re.search(r'할\s*[인증]\s*율[^\d\+\-]*([\-\+]?[0-9\.]+)', raw_text)
                    if disc:
                        extracted['discount'] = disc.group(1).strip() + "%"
                    
                    # 4. 날짜 추출
                    board = re.search(r'이사회결의일[^\d]*(\d{4}[\-\.년\s]+\d{1,2}[\-\.월\s]+\d{1,2})', raw_text)
                    if board:
                        extracted['board_date'] = fix_date(board.group(1).strip())
                    
                    pay = re.search(r'납\s*입\s*일[^\d]*(\d{4}[\-\.년\s]+\d{1,2}[\-\.월\s]+\d{1,2})', raw_text)
                    if pay:
                        extracted['pay_date'] = fix_date(pay.group(1).strip())
                    
                    div = re.search(r'배당기산일[^\d]*(\d{4}[\-\.년\s]+\d{1,2}[\-\.월\s]+\d{1,2})', raw_text)
                    if div:
                        extracted['div_date'] = fix_date(div.group(1).strip())
                    
                    list_d = re.search(r'상장\s*예정일[^\d]*(\d{4}[\-\.년\s]+\d{1,2}[\-\.월\s]+\d{1,2})', raw_text)
                    if list_d:
                        extracted['list_date'] = fix_date(list_d.group(1).strip())
                    
                    # 5. 투자자
                    if "제3자배정" in raw_text:
                        extracted['investor'] = "제3자배정 (원문참조)"

    except Exception as e:
        print(f"문서 XML 에러 ({rcept_no}): {e}")
        
    return extracted

# 안전한 숫자 변환 함수
def to_int(val):
    try:
        if pd.isna(val) or str(val).strip() == '':
            return 0
        return int(float(str(val).replace(',', '').strip()))
    except:
        return 0

def get_and_update_yusang():
    start_date = '20260201'
    end_date = '20260228'

    print(f"{start_date} ~ {end_date} 유상증자 공시 탐색 중...")

    list_url = "https://opendart.fss.or.kr/api/list.json"
    list_params = {
        'crtfc_key': dart_key,
        'bgn_de': start_date,
        'end_de': end_date,
        'pblntf_ty': 'B',
        'pblntf_detail_ty': 'B001',
        'page_count': '100'
    }
    all_filings = fetch_dart_json(list_url, list_params)

    if all_filings.empty:
        print("최근 지정 기간 내 주요사항보고서가 없습니다.")
        return

    df_filtered = all_filings[
        all_filings['report_nm'].str.contains('유상증자결정', na=False)
    ].copy()

    if df_filtered.empty:
        print("ℹ️ 유상증자 공시가 없습니다.")
        return
        
    corp_codes = df_filtered['corp_code'].unique()
    detail_dfs = []
    
    for code in corp_codes:
        detail_params = {
            'crtfc_key': dart_key,
            'corp_code': code,
            'bgn_de': start_date,
            'end_de': end_date
        }
        df_detail = fetch_dart_json('https://opendart.fss.or.kr/api/piicDecsn.json', detail_params)
        if not df_detail.empty:
            detail_dfs.append(df_detail)
            
    if not detail_dfs:
        print("ℹ️ 상세 데이터를 불러올 수 없습니다.")
        return
        
    df_combined = pd.concat(detail_dfs, ignore_index=True)

    # report_nm도 같이 병합
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
            rcept_val = str(row_data[NEW_RCEPT_IDX]).strip()   # 새 구조
        elif len(row_data) > OLD_RCEPT_IDX and str(row_data[OLD_RCEPT_IDX]).strip():
            rcept_val = str(row_data[OLD_RCEPT_IDX]).strip()   # 옛 구조

        if rcept_val:
            existing_data_dict[rcept_val] = {
                'row_idx': idx + 1,
                'data': [str(x).strip() for x in row_data]
            }
            
    data_to_add = []
    cls_map = {'Y': '유가', 'K': '코스닥', 'N': '코넥스', 'E': '기타'}
    
    for _, row in df_merged.iterrows():
        rcept_no = str(row.get('rcept_no', ''))
        corp_name = row.get('corp_name', '')
        report_nm = row.get('report_nm', '')
        
        xml_data = extract_xml_details(dart_key, rcept_no)
        
        # 1. 상장시장
        market = cls_map.get(row.get('corp_cls', ''), '기타')
        method = row.get('ic_mthn', '')
        
        # 2. 주식수
        ostk = to_int(row.get('nstk_ostk_cnt'))
        estk = to_int(row.get('nstk_estk_cnt'))
        new_shares = ostk + estk
        product = "보통주" if ostk > 0 else "기타주"
        
        old_ostk = to_int(row.get('bfic_tisstk_ostk'))
        old_estk = to_int(row.get('bfic_tisstk_estk'))
        old_shares = old_ostk + old_estk
        
        new_shares_str = f"{new_shares:,}"
        old_shares_str = f"{old_shares:,}"
        
        # 3. 증자비율
        ratio = f"{(new_shares / old_shares * 100):.2f}%" if old_shares > 0 else "-"
        
        # 4. 확정발행금액
        fclt = to_int(row.get('fdpp_fclt'))
        bsninh = to_int(row.get('fdpp_bsninh'))
        op = to_int(row.get('fdpp_op'))
        dtrp = to_int(row.get('fdpp_dtrp'))
        ocsa = to_int(row.get('fdpp_ocsa'))
        etc = to_int(row.get('fdpp_etc'))
        
        total_amt = fclt + bsninh + op + dtrp + ocsa + etc
        total_amt_uk = f"{(total_amt / 100000000):,.2f}" if total_amt > 0 else "0.00"
        
        # 자금용도 추출
        purposes = []
        if fclt > 0: purposes.append("시설")
        if bsninh > 0: purposes.append("영업양수")
        if op > 0: purposes.append("운영")
        if dtrp > 0: purposes.append("채무상환")
        if ocsa > 0: purposes.append("타법인증권")
        if etc > 0: purposes.append("기타")
        purpose_str = ", ".join(purposes)
        
        link = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"
        
        # 회사명 다음에 보고서명 추가
        new_row = [
            corp_name,                  # 1 회사명
            report_nm,                  # 2 보고서명
            market,                     # 3 상장시장
            xml_data['board_date'],     # 4 최초 이사회결의일
            method,                     # 5 증자방식
            product,                    # 6 발행주식종류
            new_shares_str,             # 7 신규발행주식수
            xml_data['issue_price'],    # 8 확정발행가
            xml_data['base_price'],     # 9 기준주가
            total_amt_uk,               # 10 확정발행금액(억원)
            xml_data['discount'],       # 11 할인율
            old_shares_str,             # 12 증자전 주식수
            ratio,                      # 13 증자비율
            xml_data['pay_date'],       # 14 납입일
            xml_data['div_date'],       # 15 배당기산일
            xml_data['list_date'],      # 16 상장예정일
            xml_data['board_date'],     # 17 이사회결의일
            purpose_str,                # 18 자금용도
            xml_data['investor'],       # 19 투자자
            link,                       # 20 링크
            rcept_no                    # 21 접수번호
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
                print(f" 🔄 {corp_name}: 데이터 변경 감지! 최신 내용으로 자동 덮어쓰기 완료 (행: {row_idx})")
            else:
                print(f" ⏩ {corp_name}: 변경사항 없음 (패스)")
                
        else:
            print(f" 🆕 {corp_name}: 신규 공시 발견! 추가 대기 중...")
            data_to_add.append(new_row)
        
    if data_to_add:
        worksheet.append_rows(data_to_add)
        print(f"✅ 유상증자: 신규 데이터 {len(data_to_add)}건 일괄 추가 완료!")
    else:
        print("✅ 유상증자: 새로 추가할 공시는 없으며 데이터 최신화 점검을 마쳤습니다.")

if __name__ == "__main__":
    get_and_update_yusang()
