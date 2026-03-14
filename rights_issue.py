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
