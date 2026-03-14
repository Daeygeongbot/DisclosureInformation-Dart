#주식연계채권 전용 코드
import os
import json
import gspread
import pandas as pd
import requests
import zipfile
import io
import re
import time  # 업데이트 시 구글 API 과부하 방지용으로 추가
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

# 시트 컬럼 수
TOTAL_COLS = 26
RCEPT_NO_COL_IDX = 25  # 0-based index (26번째 컬럼 = 접수번호)

# --- [JSON 파싱] ---
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

# --- [list.json 전체 페이지 수집] ---
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

# --- [채권 전용 XML 원문 족집게 파싱 (콜/풋옵션 내용 추출 500자로 대폭 확장)] ---
# (작성해주신 원본 함수 그대로 유지)
def extract_bond_xml_details(api_key, rcept_no):
    url = "https://opendart.fss.or.kr/api/document.xml"
    params = {'crtfc_key': api_key, 'rcept_no': rcept_no}
    
    extracted = {
        'put_option': '없음', 'call_option': '없음', 
        'call_ratio': '-', 'ytc': '-', 'investor': '원문참조'
    }
    
    try:
        res = requests.get(url, params=params, stream=True, timeout=30)
        if res.status_code == 200:
            with zipfile.ZipFile(io.BytesIO(res.content)) as z:
                xml_filename = [name for name in z.namelist() if name.endswith('.xml')][0]
                with z.open(xml_filename) as f:
                    xml_content = f.read().decode('utf-8')
                    soup = BeautifulSoup(xml_content, 'html.parser')
                    
                    for tag in soup.find_all(['td', 'th', 'p', 'div']):
                        tag.append(' ')
                        
                    raw_text = soup.get_text(separator=' ', strip=True)
                    clean_text = re.sub(r'\s+', ' ', raw_text)
                    
                    # 💡 1. Put Option (조기상환청구권) : 500자로 넉넉하게 추출
                    put_match = re.search(r'(조기상환\s*청구권.{0,500})', clean_text)
                    if put_match:
                        extracted['put_option'] = put_match.group(1).strip() + "..."
                        
                    # 💡 2. Call Option (매도청구권) : 500자로 넉넉하게 추출
                    call_match = re.search(r'(매도\s*청구권.{0,500})', clean_text)
                    if call_match:
                        extracted['call_option'] = call_match.group(1).strip() + "..."
                        
                        # Call 비율 추출
                        ratio_match = re.search(r'([0-9]{1,3}(?:\.[0-9]+)?)\s*%', call_match.group(0))
                        if ratio_match:
                            extracted['call_ratio'] = ratio_match.group(1) + '%'
                            
                    # 3. YTC (매도청구권 수익률)
                    ytc_match = re.search(r'매도청구권.*?수익률.{0,50}?([0-9]{1,2}(?:\.[0-9]+)?)\s*%', clean_text)
                    if ytc_match:
                        extracted['ytc'] = ytc_match.group(1) + '%'
                        
                    # 4. 투자자 (대상자) 추출 시도
                    inv_match = re.search(r'배정\s*대상자.{0,100}?(주식회사\s*\S+|\S+\s*투자조합|\S+\s*펀드|[가-힣]{2,4})', clean_text)
                    if inv_match:
                        extracted['investor'] = inv_match.group(1).strip()
                    elif "제3자배정" in clean_text:
                        extracted['investor'] = "제3자배정 (원문참조)"

    except Exception as e:
        print(f"채권 XML 에러 ({rcept_no}): {e}")
        
    return extracted

# 안전한 숫자 변환 함수
def to_int(val):
    try:
        if pd.isna(val) or str(val).strip() == '':
            return 0
        return int(float(str(val).replace(',', '').strip()))
    except:
        return 0


# 💡 [수정] 보고서명 컬럼 추가
def make_row_data(row, xml_data, config, cls_map):
    f_map = config['fields']
    rcept_no = str(row.get('rcept_no', ''))
    corp_name = row.get('corp_name', '')
    report_nm = row.get('report_nm', '')
    
    fclt = to_int(row.get('fdpp_fclt'))
    bsninh = to_int(row.get('fdpp_bsninh'))
    op = to_int(row.get('fdpp_op'))
    dtrp = to_int(row.get('fdpp_dtrp'))
    ocsa = to_int(row.get('fdpp_ocsa'))
    etc = to_int(row.get('fdpp_etc'))
    
    purposes = []
    if fclt > 0: purposes.append("시설")
    if bsninh > 0: purposes.append("영업양수")
    if op > 0: purposes.append("운영")
    if dtrp > 0: purposes.append("채무상환")
    if ocsa > 0: purposes.append("타법인증권")
    if etc > 0: purposes.append("기타")
    purpose_str = ", ".join(purposes) if purposes else "-"

    face_value = to_int(row.get('bd_fta'))
    face_value_str = f"{face_value:,}" if face_value > 0 else "-"
    
    bd_tm = str(row.get('bd_tm', '')).strip()
    bd_knd = str(row.get('bd_knd', '')).strip()
    product_name = f"제{bd_tm}회차 {bd_knd}" if bd_tm else bd_knd

    shares = to_int(row.get(f_map['shares']))
    shares_str = f"{shares:,}" if shares > 0 else "-"
    
    refix_val = to_int(row.get(f_map['refix'])) if f_map['refix'] else 0
    refix_str = f"{refix_val:,}" if refix_val > 0 else "-"
    
    price_val = to_int(row.get(f_map['price']))
    price_str = f"{price_val:,}" if price_val > 0 else "-"

    link = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"
    
    return [
        config['type'],                               # 1. 구분 (CB, BW, EB)
        corp_name,                                    # 2. 회사명
        report_nm,                                    # 3. 보고서명
        cls_map.get(row.get('corp_cls', ''), '기타'), # 4. 상장시장
        str(row.get('bddd', '-')),                    # 5. 최초 이사회결의일
        face_value_str,                               # 6. 권면총액(원)
        str(row.get('bd_intr_ex', '-')),              # 7. Coupon (표면이자율)
        str(row.get('bd_intr_sf', '-')),              # 8. YTM (만기이자율)
        str(row.get('bd_mtd', '-')),                  # 9. 만기
        str(row.get(f_map['start'], '-')),            # 10. 전환청구 시작
        str(row.get(f_map['end'], '-')),              # 11. 전환청구 종료
        xml_data['put_option'],                       # 12. Put Option
        xml_data['call_option'],                      # 13. Call Option
        xml_data['call_ratio'],                       # 14. Call 비율
        xml_data['ytc'],                              # 15. YTC
        str(row.get('bdis_mthn', '-')),               # 16. 모집방식
        product_name,                                 # 17. 발행상품
        price_str,                                    # 18. 행사(전환)가액(원)
        shares_str,                                   # 19. 전환주식수
        str(row.get(f_map['ratio'], '-')),            # 20. 주식총수대비 비율
        refix_str,                                    # 21. Refixing Floor
        str(row.get('pymd', '-')),                    # 22. 납입일
        purpose_str,                                  # 23. 자금용도
        xml_data['investor'],                         # 24. 투자자
        link,                                         # 25. 링크
        rcept_no                                      # 26. 접수번호
    ]


def get_and_update_bonds():
    start_date = '20260201'
    end_date = '20260228'

    print(f"{start_date} ~ {end_date} 주식연계채권(CB, BW, EB) 공시 탐색 중...")

    # 공시 목록 호출
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

    # 채권 종류별 설정값 (API 필드명이 다르므로 매핑)
    bond_configs = [
        {
            'type': 'CB',
            'keyword': '전환사채권발행결정',
            'endpoint': 'cvbdIsDecsn',
            'fields': {
                'price': 'cv_prc',
                'shares': 'cvisstk_cnt',
                'ratio': 'cvisstk_tisstk_vs',
                'start': 'cvrqpd_bgd',
                'end': 'cvrqpd_edd',
                'refix': 'act_mktprcfl_cvprc_lwtrsprc'
            }
        },
        {
            'type': 'BW',
            'keyword': '신주인수권부사채권발행결정',
            'endpoint': 'bdwtIsDecsn',
            'fields': {
                'price': 'ex_prc',
                'shares': 'nstk_isstk_cnt',
                'ratio': 'nstk_isstk_tisstk_vs',
                'start': 'expd_bgd',
                'end': 'expd_edd',
                'refix': 'act_mktprcfl_cvprc_lwtrsprc'
            }
        },
        {
            'type': 'EB',
            'keyword': '교환사채권발행결정',
            'endpoint': 'exbdIsDecsn',
            'fields': {
                'price': 'ex_prc',
                'shares': 'extg_stkcnt',
                'ratio': 'extg_tisstk_vs',
                'start': 'exrqpd_bgd',
                'end': 'exrqpd_edd',
                'refix': ''
            }
        }
    ]

    worksheet = sh.worksheet('D_주식연계채권')
    cls_map = {'Y': '유가', 'K': '코스닥', 'N': '코넥스', 'E': '기타'}

    # 시트의 기존 데이터 읽기
    all_sheet_data = worksheet.get_all_values()
    rcept_row_map = {
        row[RCEPT_NO_COL_IDX]: i + 1
        for i, row in enumerate(all_sheet_data)
        if len(row) > RCEPT_NO_COL_IDX
    }
    existing_rcept_nos = list(rcept_row_map.keys())

    for config in bond_configs:
        print(f"\n[{config['type']}] 데이터 확인 중...")
        df_filtered = all_filings[all_filings['report_nm'].str.contains(config['keyword'], na=False)].copy()
        
        if df_filtered.empty:
            print(f"ℹ️ {config['type']} 공시가 없습니다.")
            continue

        # rcept_no / report_nm 매핑용
        df_filtered['rcept_no'] = df_filtered['rcept_no'].astype(str)
        df_report_map = df_filtered[['rcept_no', 'report_nm']].drop_duplicates(subset=['rcept_no'])
            
        corp_codes = df_filtered['corp_code'].astype(str).unique()
        detail_dfs = []

        # 상세 API는 최초접수일 기준 이슈가 있을 수 있어 조회 시작일을 넉넉하게 확장
        detail_start_date = (datetime.strptime(start_date, "%Y%m%d") - timedelta(days=180)).strftime("%Y%m%d")
        
        for code in corp_codes:
            detail_params = {
                'crtfc_key': dart_key,
                'corp_code': code,
                'bgn_de': detail_start_date,
                'end_de': end_date
            }
            df_detail = fetch_dart_json(
                f"https://opendart.fss.or.kr/api/{config['endpoint']}.json",
                detail_params
            )
            if not df_detail.empty:
                detail_dfs.append(df_detail)
                
        if not detail_dfs:
            continue
            
        df_combined = pd.concat(detail_dfs, ignore_index=True)
        df_combined['rcept_no'] = df_combined['rcept_no'].astype(str)
        
        target_rcept_nos = df_filtered['rcept_no'].unique()
        df_merged = df_combined[df_combined['rcept_no'].isin(target_rcept_nos)].copy()

        # 💡 [추가] report_nm 붙이기
        df_merged = df_merged.merge(df_report_map, on='rcept_no', how='left')
        
        # ========================================================
        # 🟢 1. 신규 데이터 추가 로직
        # ========================================================
        new_data_df = df_merged[~df_merged['rcept_no'].astype(str).isin(existing_rcept_nos)]
        
        data_to_add = []
        for _, row in new_data_df.iterrows():
            rcept_no = str(row.get('rcept_no', ''))
            print(f" -> [신규] {row.get('corp_name', '')} 데이터 포매팅 중...")
            xml_data = extract_bond_xml_details(dart_key, rcept_no)
            new_row = make_row_data(row, xml_data, config, cls_map)
            data_to_add.append(new_row)
            
        if data_to_add:
            worksheet.append_rows(data_to_add)
            print(f"✅ {config['type']}: 신규 데이터 {len(data_to_add)}건 추가 완료!")

        # 신규 append 후 row map 갱신
        if data_to_add:
            all_sheet_data = worksheet.get_all_values()
            rcept_row_map = {
                row[RCEPT_NO_COL_IDX]: i + 1
                for i, row in enumerate(all_sheet_data)
                if len(row) > RCEPT_NO_COL_IDX
            }
            existing_rcept_nos = list(rcept_row_map.keys())

        # ========================================================
        # 🔄 2. 기존 데이터 재검사 및 덮어쓰기 로직
        # ========================================================
        existing_data_df = df_merged[df_merged['rcept_no'].astype(str).isin(existing_rcept_nos)]
        update_count = 0
        
        for _, row in existing_data_df.iterrows():
            rcept_no = str(row.get('rcept_no', ''))
            row_idx = rcept_row_map.get(rcept_no)
            if not row_idx:
                continue

            # 시트 현재 값
            sheet_row = all_sheet_data[row_idx - 1]
            
            # 최신 값 재구성
            xml_data = extract_bond_xml_details(dart_key, rcept_no)
            new_row = make_row_data(row, xml_data, config, cls_map)
            
            # 길이 26으로 맞춘 뒤 비교
            sheet_row_padded = sheet_row + [''] * (TOTAL_COLS - len(sheet_row))
            new_row_str = [str(x) for x in new_row]

            if sheet_row_padded != new_row_str:
                corp_name = row.get('corp_name', '')
                print(f" 🔄 [업데이트] {corp_name} 값이 변경/확정되었습니다. 시트를 덮어씁니다.")
                worksheet.update(values=[new_row], range_name=f'A{row_idx}')
                update_count += 1
                time.sleep(1)

        if update_count > 0:
            print(f"✅ {config['type']}: 기존 데이터 {update_count}건 자동 업데이트 완료!")


if __name__ == "__main__":
    get_and_update_bonds()
