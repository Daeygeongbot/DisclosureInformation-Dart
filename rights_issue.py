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
                    
                    # 💡 [추가] 날짜를 YYYY년 MM월 DD일 로 깔끔하게 강제 포맷팅하는 헬퍼 함수
                    def fix_date(raw_date_str):
                        if not raw_date_str: return '-'
                        # 숫자만 3개(연, 월, 일) 뽑아냄
                        nums = re.findall(r'\d+', raw_date_str)
                        if len(nums) >= 3:
                            return f"{nums[0]}년 {nums[1].zfill(2)}월 {nums[2].zfill(2)}일"
                        return raw_date_str + "일" # 숫자가 부족하면 임시방편으로 '일'만 붙임
                    
                    # 1. 확정발행가 추출 (글자 사이 잡문자 무시하고 첫 숫자 매칭)
                    issue = re.search(r'발행가액[^\d]*([0-9]{1,3}(?:,[0-9]{3})*)', raw_text)
                    if issue: extracted['issue_price'] = issue.group(1).strip()
                    
                    # 2. 기준주가 추출
                    base = re.search(r'기준주가[^\d]*([0-9]{1,3}(?:,[0-9]{3})*)', raw_text)
                    if base: extracted['base_price'] = base.group(1).strip()
                    
                    # 3. 할인/할증률 추출 (마이너스 기호가 살도록 정규식 핀셋 수정 완료!)
                    disc = re.search(r'할\s*[인증]\s*율[^\d\+\-]*([\-\+]?[0-9\.]+)', raw_text)
                    if disc: extracted['discount'] = disc.group(1).strip() + "%"
                    
                    # 4. 날짜 추출 (이사회, 납입일, 배당기산일, 상장예정일) + 💡 fix_date 적용!
                    board = re.search(r'이사회결의일[^\d]*(\d{4}[\-\.년\s]+\d{1,2}[\-\.월\s]+\d{1,2})', raw_text)
                    if board: extracted['board_date'] = fix_date(board.group(1).strip())
                    
                    pay = re.search(r'납\s*입\s*일[^\d]*(\d{4}[\-\.년\s]+\d{1,2}[\-\.월\s]+\d{1,2})', raw_text)
                    if pay: extracted['pay_date'] = fix_date(pay.group(1).strip())
                    
                    div = re.search(r'배당기산일[^\d]*(\d{4}[\-\.년\s]+\d{1,2}[\-\.월\s]+\d{1,2})', raw_text)
                    if div: extracted['div_date'] = fix_date(div.group(1).strip())
                    
                    list_d = re.search(r'상장\s*예정일[^\d]*(\d{4}[\-\.년\s]+\d{1,2}[\-\.월\s]+\d{1,2})', raw_text)
                    if list_d: extracted['list_date'] = fix_date(list_d.group(1).strip())
                    
                    # 5. 투자자
                    if "제3자배정" in raw_text: extracted['investor'] = "제3자배정 (원문참조)"

    except Exception as e:
        print(f"문서 XML 에러 ({rcept_no}): {e}")
        
    return extracted

# 안전한 숫자 변환 함수
def to_int(val):
    try:
        if pd.isna(val) or str(val).strip() == '': return 0
        return int(float(str(val).replace(',', '').strip()))
    except:
        return 0

def get_and_update_yusang():
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y%m%d')

    print("최근 7일 유상증자 공시 탐색 중 (데이터 최신화 검증 로직 포함)...")
    
    list_url = "https://opendart.fss.or.kr/api/list.json"
    list_params = {
        'crtfc_key': dart_key, 'bgn_de': start_date, 'end_de': end_date, 
        'pblntf_ty': 'B', 'pblntf_detail_ty': 'B001', 'page_count': '100'
    }
    all_filings = fetch_dart_json(list_url, list_params)

    if all_filings.empty:
        print("최근 7일간 주요사항보고서가 없습니다.")
        return

    df_filtered = all_filings[all_filings['report_nm'].str.contains('유상증자결정', na=False)]
    if df_filtered.empty:
        print("ℹ️ 유상증자 공시가 없습니다.")
        return
        
    corp_codes = df_filtered['corp_code'].unique()
    detail_dfs = []
    
    for code in corp_codes:
        detail_params = {'crtfc_key': dart_key, 'corp_code': code, 'bgn_de': start_date, 'end_de': end_date}
        df_detail = fetch_dart_json('https://opendart.fss.or.kr/api/piicDecsn.json', detail_params)
        if not df_detail.empty:
            detail_dfs.append(df_detail)
            
    if not detail_dfs:
        print("ℹ️ 상세 데이터를 불러올 수 없습니다.")
        return
        
    df_combined = pd.concat(detail_dfs, ignore_index=True)
    
    # 상장시장(corp_cls) 이름 충돌 방지를 위해 목록에서는 rcept_no만 가져와 병합
    df_merged = pd.merge(df_combined, df_filtered[['rcept_no']], on='rcept_no', how='inner')
    
    worksheet = sh.worksheet('유상증자')
    
    # 💡 [추가] Recheck + Diff + Update 로직을 위한 기존 시트 데이터 전체 불러오기
    all_sheet_data = worksheet.get_all_values()
    existing_data_dict = {}
    
    # 구글 시트에 있는 데이터를 { '접수번호': { '행번호': 2, '데이터': ['값1', '값2'...] } } 형태로 메모리에 저장
    for idx, row_data in enumerate(all_sheet_data):
        if len(row_data) >= 20: # 20번째 칸(T열)이 접수번호
            rcept_val = str(row_data[19]).strip()
            existing_data_dict[rcept_val] = {
                'row_idx': idx + 1, # 구글 시트는 1행부터 시작하므로 +1
                'data': [str(x).strip() for x in row_data] # 공백 제거 후 문자열로 저장
            }
            
    data_to_add = []
    cls_map = {'Y': '유가', 'K': '코스닥', 'N': '코넥스', 'E': '기타'}
    
    # 💡 [수정] 필터링 없이 일단 최근 7일치 공시 전체를 훑으며 변경사항(Diff)이 있는지 검사합니다.
    for _, row in df_merged.iterrows():
        rcept_no = str(row.get('rcept_no', ''))
        corp_name = row.get('corp_name', '')
        
        xml_data = extract_xml_details(dart_key, rcept_no)
        
        # 1. 상장시장 (에러 해결)
        market = cls_map.get(row.get('corp_cls', ''), '기타')
        method = row.get('ic_mthn', '')
        
        # 2. 주식수 & 천 단위 콤마
        ostk = to_int(row.get('nstk_ostk_cnt'))
        estk = to_int(row.get('nstk_estk_cnt'))
        new_shares = ostk + estk
        product = "보통주" if ostk > 0 else "기타주"
        
        old_ostk = to_int(row.get('bfic_tisstk_ostk'))
        old_estk = to_int(row.get('bfic_tisstk_estk'))
        old_shares = old_ostk + old_estk
        
        new_shares_str = f"{new_shares:,}"  # 쉼표 추가
        old_shares_str = f"{old_shares:,}"  # 쉼표 추가
        
        # 3. 증자비율 (%)
        ratio = f"{(new_shares / old_shares * 100):.2f}%" if old_shares > 0 else "-"
        
        # 4. 확정발행금액 (억원 단위, 소수점 2자리 세밀하게)
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
        
        # 새롭게 만들어낸 최신 팩트 데이터 한 줄
        new_row = [
            corp_name,                  # 1
            market,                     # 2 (복구됨)
            xml_data['board_date'],     # 3
            method,                     # 4
            product,                    # 5
            new_shares_str,             # 6 (쉼표 추가)
            xml_data['issue_price'],    # 7 (업그레이드)
            xml_data['base_price'],     # 8 (업그레이드)
            total_amt_uk,               # 9 (소수점 추가)
            xml_data['discount'],       # 10 (마이너스 부호 추가됨!)
            old_shares_str,             # 11 (쉼표 추가)
            ratio,                      # 12 (비율 복구)
            xml_data['pay_date'],       # 13
            xml_data['div_date'],       # 14
            xml_data['list_date'],      # 15
            xml_data['board_date'],     # 16
            purpose_str,                # 17
            xml_data['investor'],       # 18
            link,                       # 19
            rcept_no                    # 20
        ]
        
        # 비교를 위해 모든 데이터를 문자열(String)로 변환
        new_row_str = [str(x).strip() for x in new_row]
        
        # 💡 [핵심 로직] 기존 시트에 있는 데이터인지 검증 및 업데이트
        if rcept_no in existing_data_dict:
            existing_row_str = existing_data_dict[rcept_no]['data']
            
            # 길이를 맞춰서 1:1 비교를 수행합니다 (에러 방지용 패딩)
            existing_row_str += [''] * (len(new_row_str) - len(existing_row_str))
            existing_row_str = existing_row_str[:len(new_row_str)]
            
            # Diff 검사: 하나라도 값이 다르다면 업데이트 실행!
            if new_row_str != existing_row_str:
                row_idx = existing_data_dict[rcept_no]['row_idx']
                try:
                    worksheet.update(range_name=f'A{row_idx}:T{row_idx}', values=[new_row])
                except TypeError:
                    worksheet.update(f'A{row_idx}:T{row_idx}', [new_row])
                print(f" 🔄 {corp_name}: 데이터 변경 감지! 최신 내용으로 자동 덮어쓰기 완료 (행: {row_idx})")
            else:
                print(f" ⏩ {corp_name}: 변경사항 없음 (패스)")
                
        else:
            # 시트에 아예 없는 접수번호라면 신규 데이터 바구니에 담기
            print(f" 🆕 {corp_name}: 신규 공시 발견! 추가 대기 중...")
            data_to_add.append(new_row)
        
    # 신규 데이터가 있으면 맨 밑에 일괄 추가
    if data_to_add:
        worksheet.append_rows(data_to_add)
        print(f"✅ 유상증자: 신규 데이터 {len(data_to_add)}건 일괄 추가 완료!")
    else:
        print("✅ 유상증자: 새로 추가할 공시는 없으며 데이터 최신화 점검을 마쳤습니다.")

if __name__ == "__main__":
    get_and_update_yusang()
