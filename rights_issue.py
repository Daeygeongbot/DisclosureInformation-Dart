import os
import json
import io
import re
import zipfile
from datetime import datetime, timedelta

import gspread
import pandas as pd
import requests
from bs4 import BeautifulSoup


# ==========================================================
# 1. GitHub Secrets 설정값
# ==========================================================
dart_key = os.environ["DART_API_KEY"]
service_account_str = os.environ["GOOGLE_CREDENTIALS_JSON"]
sheet_id = os.environ["GOOGLE_SHEET_ID"]


# ==========================================================
# 2. 구글 시트 인증
# ==========================================================
creds = json.loads(service_account_str)
gc = gspread.service_account_from_dict(creds)
sh = gc.open_by_key(sheet_id)


# ==========================================================
# 3. 시트 구조
# ==========================================================
TOTAL_COLS = 21
NEW_RCEPT_IDX = 20   # 21번째 컬럼 (0-based)
OLD_RCEPT_IDX = 19   # 기존 구조 호환용

# 기존 행에서는 아래 5개 컬럼만 업데이트
TARGET_COLS = {
    6: "신규발행주식수",
    7: "확정발행가(원)",
    8: "기준주가",
    9: "확정발행금액(억원)",
    11: "증자전 주식수",
}


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


def normalize_label_key(text):
    s = normalize_text(text)

    # "6. 신주 발행가액" 같은 앞 번호 제거
    s = re.sub(r"^\s*\d{1,2}\s*[\.\)]\s*", "", s)

    s = compact_text(s)
    s = re.sub(r"[^\w가-힣]", "", s)
    return s


def format_date_display(raw_date_str):
    if not raw_date_str:
        return "-"
    nums = re.findall(r"\d+", str(raw_date_str))
    if len(nums) >= 3:
        return f"{nums[0]}년 {nums[1].zfill(2)}월 {nums[2].zfill(2)}일"
    return "-"


def to_int(val):
    try:
        if pd.isna(val) or str(val).strip() == "":
            return 0
        return int(float(str(val).replace(",", "").strip()))
    except:
        return 0


def to_float(val):
    try:
        if pd.isna(val) or str(val).strip() == "":
            return 0.0
        return float(str(val).replace(",", "").replace("%", "").strip())
    except:
        return 0.0


def has_meaningful_target_value(col_idx, value):
    s = str(value).strip()
    if s in {"", "-", "nan", "None"}:
        return False

    if col_idx in [6, 11]:
        return to_int(s) > 0

    if col_idx in [7, 8, 9]:
        return to_float(s) > 0

    return True


def keep_existing_if_invalid(col_idx, new_val, old_val):
    if has_meaningful_target_value(col_idx, new_val):
        return str(new_val).strip()

    old_s = str(old_val).strip()
    if old_s not in {"", "nan", "None"}:
        return old_s

    return str(new_val).strip()


# ==========================================================
# DART API
# ==========================================================
def fetch_dart_json(url, params):
    try:
        res = requests.get(url, params=params, timeout=20)
        if res.status_code == 200:
            data = res.json()
            if data.get("status") == "000" and "list" in data:
                return pd.DataFrame(data["list"])
    except Exception as e:
        print(f"JSON API 에러: {e}")
    return pd.DataFrame()


def fetch_dart_list_all(url, params):
    frames = []
    page_no = 1
    total_page = 1

    while page_no <= total_page:
        page_params = params.copy()
        page_params["page_no"] = page_no
        page_params["page_count"] = "100"

        try:
            res = requests.get(url, params=page_params, timeout=20)
            if res.status_code != 200:
                print(f"list.json HTTP 에러: {res.status_code}")
                break

            data = res.json()

            if data.get("status") == "013":
                break

            if data.get("status") != "000":
                print(f"list.json API 에러: {data.get('status')} / {data.get('message')}")
                break

            total_page = int(data.get("total_page", 1))

            if data.get("list"):
                frames.append(pd.DataFrame(data["list"]))

            page_no += 1

        except Exception as e:
            print(f"list.json 페이지 수집 에러: {e}")
            break

    if frames:
        return pd.concat(frames, ignore_index=True)

    return pd.DataFrame()


# ==========================================================
# XML 파싱용 유틸
# ==========================================================
def get_table_rows_from_soup(soup):
    tables = []

    for table in soup.find_all("table"):
        table_rows = []
        for tr in table.find_all("tr"):
            row = []
            for cell in tr.find_all(["th", "td"]):
                txt = normalize_text(cell.get_text(" ", strip=True))
                if txt:
                    row.append(txt)
            if row:
                table_rows.append(row)

        if table_rows:
            tables.append(table_rows)

    return tables


def extract_numeric_tokens(text):
    """
    숫자 토큰 추출.
    - "6. 신주 발행가액" 앞 번호는 제거
    - 단독 1자리 숫자(6, 7 등)는 버림
    """
    s = normalize_text(text)
    if not s:
        return []

    # 앞 section 번호 제거
    s = re.sub(r"^\s*\d{1,2}\s*[\.\)]\s*(?=[^\d])", "", s).strip()

    # 단독 1자리 숫자는 무시 (항목번호 방지)
    if re.fullmatch(r"\d", s):
        return []

    tokens = re.findall(r"([+\-]?\d[\d,]*(?:\.\d+)?)", s)

    out = []
    for tok in tokens:
        if re.fullmatch(r"\d", tok):
            continue
        out.append(tok.strip())
    return out


def score_stock_context(text, prefer_stock):
    if not prefer_stock:
        return 0

    c = compact_text(text)

    if prefer_stock == "보통주":
        if "보통주식" in c or "보통주" in c:
            return 30
        if "기타주식" in c or "기타주" in c or "종류주식" in c or "종류주" in c:
            return -10

    if prefer_stock == "기타주":
        if "기타주식" in c or "기타주" in c or "종류주식" in c or "종류주" in c:
            return 30
        if "보통주식" in c or "보통주" in c:
            return -10

    return 0


def score_numeric_cell(row, cell_idx, prefer_stock):
    context = " ".join(row[max(0, cell_idx - 2): cell_idx + 1])
    score = score_stock_context(context, prefer_stock)

    if "원" in context:
        score += 3

    return score


def find_number_by_labels_in_tables(tables, label_aliases, prefer_stock=None, context_rows=4):
    alias_keys = [normalize_label_key(x) for x in label_aliases]
    candidates = []

    for table in tables:
        for r_idx, row in enumerate(table):
            row_text = " ".join(row)
            row_key = normalize_label_key(row_text)

            if not any(alias in row_key for alias in alias_keys):
                continue

            # 1) 같은 row에서 라벨 오른쪽 셀 우선
            for cell_idx, cell in enumerate(row):
                cell_key = normalize_label_key(cell)
                if any(alias in cell_key for alias in alias_keys):
                    for right_idx in range(cell_idx + 1, len(row)):
                        nums = extract_numeric_tokens(row[right_idx])
                        for num in nums:
                            score = 120 + score_numeric_cell(row, right_idx, prefer_stock)
                            candidates.append((score, num))

            # 2) 같은 row 전체
            for cell_idx, cell in enumerate(row):
                nums = extract_numeric_tokens(cell)
                for num in nums:
                    score = 90 + score_numeric_cell(row, cell_idx, prefer_stock)
                    candidates.append((score, num))

            # 3) 아래 context row 탐색
            end_idx = min(len(table), r_idx + context_rows + 1)
            for rr in range(r_idx + 1, end_idx):
                sub_row = table[rr]
                for cell_idx, cell in enumerate(sub_row):
                    nums = extract_numeric_tokens(cell)
                    for num in nums:
                        distance_penalty = (rr - r_idx) * 10
                        score = 80 - distance_penalty + score_numeric_cell(sub_row, cell_idx, prefer_stock)
                        candidates.append((score, num))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def find_number_near_labels(text, label_patterns, window=120):
    for pat in label_patterns:
        for m in re.finditer(pat, text, flags=re.IGNORECASE):
            snippet = text[m.end():m.end() + window]
            nums = extract_numeric_tokens(snippet)
            if nums:
                return nums[0]
    return None


def find_date_from_lines(lines, include_keywords, exclude_keywords=None):
    exclude_keywords = exclude_keywords or []
    include_compact = [compact_text(x) for x in include_keywords]
    exclude_compact = [compact_text(x) for x in exclude_keywords]

    for i, line in enumerate(lines):
        c_line = compact_text(line)

        if all(k in c_line for k in include_compact) and not any(k in c_line for k in exclude_compact):
            m = re.search(r"(\d{4}[년\.\-/\s]+\d{1,2}[월\.\-/\s]+\d{1,2}일?)", line)
            if m:
                return format_date_display(m.group(1))

            if i + 1 < len(lines):
                nxt = lines[i + 1]
                m2 = re.search(r"(\d{4}[년\.\-/\s]+\d{1,2}[월\.\-/\s]+\d{1,2}일?)", nxt)
                if m2:
                    return format_date_display(m2.group(1))

    return "-"


def find_line_value(lines, include_keywords, exclude_keywords=None):
    exclude_keywords = exclude_keywords or []
    include_compact = [compact_text(x) for x in include_keywords]
    exclude_compact = [compact_text(x) for x in exclude_keywords]

    for i, line in enumerate(lines):
        c_line = compact_text(line)

        if all(k in c_line for k in include_compact) and not any(k in c_line for k in exclude_compact):
            parts = re.split(r"[:：]", line, maxsplit=1)
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
def extract_xml_details(api_key, rcept_no, prefer_stock=None):
    url = "https://opendart.fss.or.kr/api/document.xml"
    params = {"crtfc_key": api_key, "rcept_no": rcept_no}

    extracted = {
        "first_board_date": "-",
        "board_date": "-",
        "issue_price": "-",
        "base_price": "-",
        "discount": "-",
        "pay_date": "-",
        "div_date": "-",
        "list_date": "-",
        "investor": "원문참조",
    }

    try:
        res = requests.get(url, params=params, stream=True, timeout=30)
        if res.status_code != 200:
            return extracted

        with zipfile.ZipFile(io.BytesIO(res.content)) as z:
            xml_files = [name for name in z.namelist() if name.endswith(".xml")]
            if not xml_files:
                return extracted

            with z.open(xml_files[0]) as f:
                xml_content = f.read().decode("utf-8", errors="ignore")

        soup = BeautifulSoup(xml_content, "html.parser")
        raw_text = soup.get_text(separator="\n")
        text = normalize_text(raw_text)
        lines = [normalize_text(x) for x in raw_text.split("\n") if normalize_text(x)]
        tables = get_table_rows_from_soup(soup)

        # 날짜
        extracted["first_board_date"] = find_date_from_lines(
            lines, include_keywords=["최초", "이사회결의일"]
        )

        extracted["board_date"] = find_date_from_lines(
            lines, include_keywords=["이사회결의일"], exclude_keywords=["최초"]
        )

        if extracted["board_date"] == "-":
            extracted["board_date"] = extracted["first_board_date"]

        extracted["pay_date"] = find_date_from_lines(lines, include_keywords=["납입일"])
        extracted["div_date"] = find_date_from_lines(lines, include_keywords=["배당기산일"])
        extracted["list_date"] = find_date_from_lines(lines, include_keywords=["상장예정일"])

        # 확정발행가(원)
        issue_price = find_number_by_labels_in_tables(
            tables,
            label_aliases=[
                "신주 발행가액",
                "1주당 발행가액",
                "발행가액",
            ],
            prefer_stock=prefer_stock,
            context_rows=4,
        )

        if not issue_price:
            issue_price = find_number_near_labels(
                text,
                label_patterns=[
                    r"신주\s*발행가액",
                    r"1주당\s*발행가액",
                    r"발행가액",
                ],
                window=120,
            )

        if issue_price and to_float(issue_price) > 0:
            extracted["issue_price"] = issue_price

        # 기준주가
        base_price = find_number_by_labels_in_tables(
            tables,
            label_aliases=[
                "기준주가",
                "산정 기준주가",
            ],
            prefer_stock=prefer_stock,
            context_rows=4,
        )

        if not base_price:
            base_price = find_number_near_labels(
                text,
                label_patterns=[
                    r"기준주가",
                    r"산정\s*기준주가",
                ],
                window=120,
            )

        if base_price and to_float(base_price) > 0:
            extracted["base_price"] = base_price

        # 할인율
        discount = find_number_near_labels(
            text,
            label_patterns=[
                r"할인율",
                r"할인률",
                r"할증율",
                r"할증률",
            ],
            window=80,
        )
        if discount:
            extracted["discount"] = f"{discount}%"

        # 투자자
        investor = (
            find_line_value(lines, ["제3자배정", "대상자"]) or
            find_line_value(lines, ["배정대상자"]) or
            find_line_value(lines, ["제3자배정"])
        )

        if investor:
            investor = re.sub(r"\s+", " ", investor).strip()
            if len(investor) <= 150:
                extracted["investor"] = investor
        else:
            if "제3자배정" in compact_text(text):
                extracted["investor"] = "제3자배정 (원문참조)"

    except Exception as e:
        print(f"문서 XML 에러 ({rcept_no}): {e}")

    return extracted


# ==========================================================
# 메인
# ==========================================================
def get_and_update_yusang():
    start_date = "20260101"
    end_date = "20260131"

    print(f"{start_date} ~ {end_date} 유상증자 공시 탐색 중...")

    list_url = "https://opendart.fss.or.kr/api/list.json"
    list_params = {
        "crtfc_key": dart_key,
        "bgn_de": start_date,
        "end_de": end_date,
        "pblntf_ty": "B",
        "pblntf_detail_ty": "B001",
    }

    all_filings = fetch_dart_list_all(list_url, list_params)

    if all_filings.empty:
        print("최근 지정 기간 내 주요사항보고서가 없습니다.")
        return

    df_filtered = all_filings[
        all_filings["report_nm"].str.contains("유상증자결정", na=False)
    ].copy()

    if df_filtered.empty:
        print("ℹ️ 유상증자 공시가 없습니다.")
        return

    corp_codes = df_filtered["corp_code"].astype(str).unique()
    detail_dfs = []

    # piicDecsn은 최초접수일 기준이라 조회 시작일을 넉넉히 확장
    detail_start_date = (
        datetime.strptime(start_date, "%Y%m%d") - timedelta(days=180)
    ).strftime("%Y%m%d")

    for code in corp_codes:
        detail_params = {
            "crtfc_key": dart_key,
            "corp_code": code,
            "bgn_de": detail_start_date,
            "end_de": end_date,
        }
        df_detail = fetch_dart_json(
            "https://opendart.fss.or.kr/api/piicDecsn.json",
            detail_params,
        )
        if not df_detail.empty:
            detail_dfs.append(df_detail)

    if not detail_dfs:
        print("ℹ️ 상세 데이터를 불러올 수 없습니다.")
        return

    df_combined = pd.concat(detail_dfs, ignore_index=True)

    df_filtered["rcept_no"] = df_filtered["rcept_no"].astype(str)
    df_combined["rcept_no"] = df_combined["rcept_no"].astype(str)

    df_merged = pd.merge(
        df_combined,
        df_filtered[["rcept_no", "report_nm"]],
        on="rcept_no",
        how="inner",
    )

    worksheet = sh.worksheet("D_유상증자")

    # 기존 시트 전체 읽기
    all_sheet_data = worksheet.get_all_values()
    existing_data_dict = {}

    for idx, row_data in enumerate(all_sheet_data):
        rcept_val = ""

        if len(row_data) > NEW_RCEPT_IDX and str(row_data[NEW_RCEPT_IDX]).strip():
            rcept_val = str(row_data[NEW_RCEPT_IDX]).strip()
        elif len(row_data) > OLD_RCEPT_IDX and str(row_data[OLD_RCEPT_IDX]).strip():
            rcept_val = str(row_data[OLD_RCEPT_IDX]).strip()

        if rcept_val:
            existing_data_dict[rcept_val] = {
                "row_idx": idx + 1,
                "data": [str(x).strip() for x in row_data],
            }

    data_to_add = []
    cls_map = {"Y": "유가", "K": "코스닥", "N": "코넥스", "E": "기타"}

    for _, row in df_merged.iterrows():
        rcept_no = str(row.get("rcept_no", "")).strip()
        corp_name = str(row.get("corp_name", "")).strip()
        report_nm = str(row.get("report_nm", "")).strip()

        # -----------------------------
        # JSON 기준 값 먼저 계산
        # -----------------------------
        ostk = to_int(row.get("nstk_ostk_cnt"))
        estk = to_int(row.get("nstk_estk_cnt"))
        new_shares = ostk + estk

        old_ostk = to_int(row.get("bfic_tisstk_ostk"))
        old_estk = to_int(row.get("bfic_tisstk_estk"))
        old_shares = old_ostk + old_estk

        prefer_stock = None
        if ostk > 0:
            prefer_stock = "보통주"
        elif estk > 0:
            prefer_stock = "기타주"

        xml_data = extract_xml_details(dart_key, rcept_no, prefer_stock=prefer_stock)

        # 상장시장 / 방식
        market = cls_map.get(str(row.get("corp_cls", "")).strip(), "기타")
        method = str(row.get("ic_mthn", "")).strip()

        # 발행주식종류
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

        # 증자비율
        ratio = f"{(new_shares / old_shares * 100):.2f}%" if old_shares > 0 else "-"

        # 확정발행금액(억원) = 4. 자금조달의 목적 합계
        fclt = to_int(row.get("fdpp_fclt"))
        bsninh = to_int(row.get("fdpp_bsninh"))
        op = to_int(row.get("fdpp_op"))
        dtrp = to_int(row.get("fdpp_dtrp"))
        ocsa = to_int(row.get("fdpp_ocsa"))
        etc = to_int(row.get("fdpp_etc"))

        total_amt = fclt + bsninh + op + dtrp + ocsa + etc
        total_amt_uk = f"{(total_amt / 100000000):,.2f}" if total_amt > 0 else "-"

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
            corp_name,                      # 1 회사명
            report_nm,                      # 2 보고서명
            market,                         # 3 상장시장
            xml_data["first_board_date"],   # 4 최초 이사회결의일
            method,                         # 5 증자방식
            product,                        # 6 발행주식종류
            new_shares_str,                 # 7 신규발행주식수
            xml_data["issue_price"],        # 8 확정발행가
            xml_data["base_price"],         # 9 기준주가
            total_amt_uk,                   # 10 확정발행금액(억원)
            xml_data["discount"],           # 11 할인율
            old_shares_str,                 # 12 증자전 주식수
            ratio,                          # 13 증자비율
            xml_data["pay_date"],           # 14 납입일
            xml_data["div_date"],           # 15 배당기산일
            xml_data["list_date"],          # 16 상장예정일
            xml_data["board_date"],         # 17 이사회결의일
            purpose_str,                    # 18 자금용도
            xml_data["investor"],           # 19 투자자
            link,                           # 20 링크
            rcept_no,                       # 21 접수번호
        ]

        new_row_str = [str(x).strip() for x in new_row]

        # -----------------------------
        # 기존 행 있으면 5개 컬럼만 업데이트
        # -----------------------------
        if rcept_no in existing_data_dict:
            row_idx = existing_data_dict[rcept_no]["row_idx"]
            existing_row = existing_data_dict[rcept_no]["data"][:]

            existing_row += [""] * (TOTAL_COLS - len(existing_row))
            existing_row = existing_row[:TOTAL_COLS]

            updated_row = existing_row[:]

            for col_idx in TARGET_COLS.keys():
                updated_row[col_idx] = keep_existing_if_invalid(
                    col_idx,
                    new_row_str[col_idx],
                    existing_row[col_idx],
                )

            changed_fields = []
            for col_idx, col_name in TARGET_COLS.items():
                old_v = str(existing_row[col_idx]).strip()
                new_v = str(updated_row[col_idx]).strip()
                if old_v != new_v:
                    changed_fields.append(f"{col_name}: {old_v} -> {new_v}")

            if changed_fields:
                try:
                    worksheet.update(range_name=f"A{row_idx}:U{row_idx}", values=[updated_row])
                except TypeError:
                    worksheet.update(f"A{row_idx}:U{row_idx}", [updated_row])

                print(f"🔄 {corp_name}: 타겟 5개 컬럼 업데이트 완료")
                for msg in changed_fields:
                    print(f"   - {msg}")
            else:
                print(f"⏩ {corp_name}: 타겟 5개 컬럼 변경사항 없음")

        # -----------------------------
        # 신규 행이면 전체 추가
        # -----------------------------
        else:
            print(f"🆕 {corp_name}: 신규 공시 발견! 추가 대기 중...")
            data_to_add.append(new_row)

    if data_to_add:
        worksheet.append_rows(data_to_add)
        print(f"✅ 유상증자: 신규 데이터 {len(data_to_add)}건 일괄 추가 완료!")
    else:
        print("✅ 유상증자: 새로 추가할 공시는 없으며 타겟 5개 컬럼 점검을 마쳤습니다.")


if __name__ == "__main__":
    get_and_update_yusang()
