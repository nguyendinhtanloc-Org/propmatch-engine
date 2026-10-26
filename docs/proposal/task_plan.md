# KẾ HOẠCH THỰC THI - 8 TUẦN

Checklist làm việc cho đồ án DE `propmatch-engine`. Mỗi tuần có **cổng kiểm tra** (gate) -
không sang tuần sau nếu chưa đạt. Dùng `[ ]` để tick.

Ngày bắt đầu: \_\_\_/\_\_/\_\_\_

---

## TUẦN 1 - HẠ TẦNG & FEASIBILITY GATE

Mục tiêu: `docker compose up` chạy được, và biết dữ liệu thật có khả thi không.

### Hạ tầng

- [ ] `infra/docker/Dockerfile.etl` - base python:3.12-slim, cài polars, sqlalchemy, boto3, minio, httpx, lxml
- [ ] `infra/docker/Dockerfile.dbt` - base python, cài dbt-core + dbt-postgres
- [ ] `src/requirements.txt` - ghim phiên bản
- [ ] `infra/postgres/init/01_extensions.sql` - CREATE EXTENSION postgis
- [ ] `infra/postgres/init/02_schema.sql` - schema raw/silver/gold/review, bảng cốt lõi
- [ ] `src/propmatch/common/config.py` - đọc env, pydantic settings
- [ ] `src/propmatch/common/logging.py` - structured log
- [ ] `src/propmatch/common/s3.py` - MinIO client (boto3, endpoint từ env)
- [ ] `src/propmatch/common/db.py` - SQLAlchemy engine + session
- [ ] `.env` - copy từ `.env.example`, đổi POSTGRES_PASSWORD
- [ ] Chạy `make up` thành công, `psql` kết nối được, PostGIS active
- [ ] `make help` hiển thị đúng

### Khảo sát nguồn dữ liệu

- [ ] Liệt kê 4-6 sàn BĐS VN ứng viên
- [ ] Đọc ToS / robots.txt từng sàn, ghi kết quả vào `docs/feasibility/to_s_review.md`
- [ ] Crawl thử 1 trang listing mỗi sàn, lưu HTML vào `data/raw/_probe/`
- [ ] Đánh giá: có chặn Cloudflare/DataDome không, cấu trúc HTML có ổn định không
- [ ] Chọn 1-2 sàn làm nguồn chính, ghi lý do vào `docs/feasibility/source_decision.md`
- [ ] Viết parser tối thiểu cho 1 sàn, trích được: id, tiêu đề, giá, diện tích, địa chỉ, loại hình, ảnh

### Feasibility gate (mục 2B.1)

Lấy mẫu 200 tin / sàn, đếm coverage:

- [ ] price coverage >= 70%
- [ ] area coverage >= 70%
- [ ] property type >= 85%
- [ ] address coverage >= 60%
- [ ] usable geolocation >= 40%
- [ ] image availability >= 40%
- [ ] joint coverage >= 40%
- [ ] `street_norm` rỗng <= 30% (nếu > 30%, Pass 2 vô dụng - phải ghi caveat)
- [ ] Cross-source overlap: >= 30 candidate pairs, >= 10 confirmed match, >= 3 khu vực
- [ ] ID stability: crawl lại 2 lần, phân biệt ID tái sử dụng vs đăng lại

**GATE:** nếu FAIL -> chuyển 80% sức lực sang synthetic + seed, ghi rõ giới hạn trong báo cáo.

### Tuần 1 xong khi

- [ ] `make up` chạy, `make seed` nạp được dữ liệu mẫu
- [ ] Có ít nhất 200 tin thật trong `data/raw/`
- [ ] Bảng feasibility có số liệu, không phải ước lượng

---

## TUẦN 2 - RAW, SILVER, IDEMPOTENCY, CHECKPOINT

- [ ] `src/propmatch/ingest/crawler.py` - rate limit 1 req/2s, rotate UA, retry 3 lần backoff
- [ ] `src/propmatch/ingest/checkpoint.py` - ghi MinIO atomic (file tạm -> rename), SHA256
- [ ] `src/propmatch/ingest/state.py` - bảng chuyển trạng thái, grace period 72h
- [ ] `src/propmatch/silver/price.py` - chuẩn hoá về `price_vnd_per_month`, price_period_raw, flags
- [ ] `src/propmatch/silver/address.py` - bỏ dấu, bỏ số nhà, chuẩn hoá viết tắt, tách street_norm
- [ ] `src/propmatch/silver/geo.py` - validate lon/lat, gắn cờ low_confidence cho ward_centroid
- [ ] `src/propmatch/silver/loader.py` - bulk write Postgres (không insert dòng lẻ)
- [ ] `src/propmatch/pipeline.py` - orchestrator, `--stage` flag
- [ ] `dbt/models/staging/` - staging view cho Silver
- [ ] Test: kill crawler giữa chừng -> resume không crawl lại
- [ ] Test: checkpoint hỏng -> fallback checkpoint gần nhất, không crash
- [ ] Test: idempotency - chạy 2 lần cùng batch, observation count không đổi

**GATE:** Raw replay OK, idempotency 100%.

---

## TUẦN 3 - BLOCKING

- [ ] `src/propmatch/er/buckets.py` - area bucket 5 mức, price bucket 5 mức, overlap 1 bucket
- [ ] `src/propmatch/er/blocking.py` - 3 pass, Union (OR)
- [ ] Canonical pair dedup `(min_id, max_id)`, loại self-pair
- [ ] Sub-block limiting: chia theo price -> area -> street, ngưỡng 1000
- [ ] Log mỗi batch: n_raw_union, n_unique_pairs, dedup_ratio, n_blocks_subdivided, n_pairs_dropped
- [ ] `src/propmatch/ingest/synthetic/seed.py` - sinh 500 cặp có nhiễu kiểm soát
- [ ] `src/propmatch/ingest/synthetic/noise.py` - 7 loại nhiễu theo proposal mục 2B.6
- [ ] Test biên bucket: 29 vs 30.5 m2, 4.9 vs 5.1 triệu - KHÔNG bị loại
- [ ] Đo candidate recall trên tập S

**GATE:** candidate recall >= 85% trên tập S, boundary test pass, dedup_ratio <= 30%.

---

## TUẦN 4 - SIMILARITY & GROUND TRUTH

- [ ] `src/propmatch/er/similarity.py` - công thức F(A,B), mẫu số động
- [ ] 4 feature: text (TF-IDF cosine), area, geo (exp decay theo met), pHash (optional)
- [ ] Minimum evidence: sum w >= 0.50 va |F| >= 2
- [ ] `src/propmatch/er/conflicts.py` - hard / soft / insufficient, quy tắc khoảng cách 100m/500m
- [ ] Lưu pair-level: algorithm_version, available_weight_sum, F(A,B), từng s_i
- [ ] `notebooks/labeling/` - Jupyter + ipywidgets, 2 cột, 3 nút Match/Non-match/Uncertain
- [ ] Gán nhãn 100-150 cặp trên dữ liệu thật (tập H)
- [ ] Tách tập theo `source_listing_id`, KHÔNG theo cluster_id

**GATE:** Pilot Gate - >= 30 match, >= 30 non-match, >= 15 cluster.

---

## TUẦN 5 - CLUSTERING & GOLD

- [ ] `src/propmatch/er/clustering.py` - dựng đồ thị, connected components
- [ ] Quét O(k^2) trong component tìm hard conflict (k <= 10)
- [ ] Conflict-driven refinement - gỡ cạnh yếu nhất, lặp lại
- [ ] Component > 10 hoặc còn unresolved -> review_queue, KHÔNG publish
- [ ] `dbt/models/marts/` - dim_canonical_listing, fact_observation, dim_ward
- [ ] `dbt/models/audit/` - kiểm tra chất lượng
- [ ] `src/propmatch/publish.py` - commit marker: manifest.json + latest.json (atomic rename)
- [ ] Reader contract: kiểm marker trước khi đọc Gold, checksum lệch -> từ chối đọc
- [ ] Kill-test: giết tiến trình giữa dbt build -> reader phải phát hiện Gold chưa publish
- [ ] Giữ N=5 run gần nhất trong MinIO, không xoá ngay

**GATE:** zero false merge trong Gold, kill-test pass.

---

## TUẦN 6 - RISK SCORING & SPATIAL ANALYTICS

- [ ] `src/propmatch/risk/scoring.py` - robust z-score / IQR theo nhóm
- [ ] Nhóm tham chiếu: loai hinh x ward x area bucket x thang
- [ ] Nhom < 20 mau -> gắn `risk_score_fallback = true`, không điền giá trị
- [ ] `dbt/models/marts/price_by_ward.sql` - gia theo phuong
- [ ] PostGIS: ST_DWithin, ST_Within, spatial join
- [ ] Dashboard Metabase: gia theo ward, phan bo risk score, so sanh candidate vs gold
- [ ] 5 cau hoi phan bien chinh (mục 15) - trang 1 phan tich

**GATE:** dashboard chạy, 5 cau hoi phan bien tra loi duoc bang du lieu that.

---

## TUẦN 7 - ĐÁNH GIÁ

- [ ] `src/propmatch/evaluation/metrics.py` - pair P/R/F1, B-cubed, Rand index
- [ ] False merge / false split - số tuyệt đối + tỷ lệ
- [ ] F1 phân tầng theo available_weight_sum (>=0.75 / 0.50-0.75 / <0.50)
- [ ] Bootstrap 95% CI
- [ ] Báo cáo RIENG tập S và tập H, không trộn
- [ ] Ablation 3 mức: baseline / +geo / +text (cùng test set, cùng candidate gen)
- [ ] pHash optional - nếu không làm, ghi rõ "baseline ER chưa có image matching"
- [ ] Splink optional - stretch goal
- [ ] Caveat list: sub-block, ward_centroid, hard conflict ngoài candidate set

**GATE:** Evaluation Gate - đủ số liệu + khoảng bất định.

---

## TUẦN 8 - BÁO CÁO & SMOKE TEST

- [ ] Viết báo cáo: vấn đề, phương pháp, kết quả, hạn chế
- [ ] Bảng so sánh Tập S vs Tập H
- [ ] `make report` xuất KPI ra `reports/`
- [ ] Smoke test trên máy sạch: clone -> make up -> make seed -> make pipeline -> make report
- [ ] Đo thời gian, phải <= 30 phút
- [ ] Kiểm tra tổng chi phí = $0
- [ ] `docs/adr/` - viết nội dung 8 ADR
- [ ] Commit lên nhánh develop, push GitHub
- [ ] Buffer - dành cho việc phát sinh

---

## CHECKLIST NGẮN - DÙNG HÀNG NGÀY

| Ngày | Việc | Xong |
|------|------|------|
| 1 | | [ ] |
| 2 | | [ ] |
| 3 | | [ ] |
| 4 | | [ ] |
| 5 | | [ ] |

---

## RỦI RO CẦN THEO DÕI

| Rủi ro | Dấu hiệu sớm | Hành động |
|--------|---------------|-----------|
| Bị chặn IP | crawl fail > 30% request | Chuyển sang nhánh synthetic ngay, ghi caveat |
| street_norm rỗng cao | > 30% mẫu rỗng | Đổi khóa Pass 2, ghi caveat |
| Trượt tiến độ | Quá 3 ngày chưa xong 1 tuần | Cắt ablation, cắt pHash, ưu tiên end-to-end |
| Tập H gán nhãn chậm | < 10 cặp/ngày | Giảm lên 100 cặp, dùng Jupyter đơn giản hơn |
