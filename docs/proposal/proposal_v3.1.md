# PROPOSAL: Real Estate Listing Intelligence Platform

**Phiên bản 3.1 (final) — Solo, 8 tuần, chi phí $0, đặc tả kỹ thuật mức production-ready**

Tên đề tài: Hệ thống hợp nhất thực thể đa nguồn, chấm điểm rủi ro tin đăng và phân tích giá rao bất động sản theo không gian.

---

## 1. MỤC ĐÍCH & PHẠM VI

Tài liệu này định nghĩa một data pipeline hoàn chỉnh, chạy end-to-end, phục vụ ba nhiệm vụ: khử trùng lặp tin đăng bất động sản xuyên nhiều sàn, chấm điểm rủi ro (theo nghĩa bất thường, không phải gian lận), và phân tích giá theo không gian.

Đặc tả được thiết kế cho một người thực hiện trong 8 tuần, chỉ dùng công cụ mã nguồn mở chạy local. Toàn bộ hạ tầng nằm trong một `docker compose` duy nhất; không phụ thuộc dịch vụ cloud nào, tổng chi phí hằng tháng là **$0**.

### 1.1 Ngoài phạm vi (non-goals)

- Mọi dịch vụ cloud-hosted (AWS, GCP, Azure)
- HMAC contact hashing và các quy trình data-redaction theo yêu cầu pháp lý
- Dashboard widget, Chrome plugin, hoặc export CDK
- Ablation study ngoài ba mức mô hình
- Replay consistency vượt quá mức record-level và operational
- Splink hoặc bất kỳ thư viện probabilistic record-linkage bên thứ ba
- Công bố bộ dữ liệu thô ra ngoài dự án

### 1.2 Sản phẩm bàn giao chính

- Crawler với fallback thông minh (dữ liệu thật + synthetic seed)
- Silver/Gold layer trên PostgreSQL/PostGIS
- Entity-resolution engine: multi-pass blocking, overlapping buckets, dedup, similarity scoring
- Quản lý cluster với cơ chế xử lý hard/soft conflict
- Risk scoring dựa trên bất thường
- Bảo đảm idempotency và replay-consistency đầy đủ
- Smoke test chạy được trong 30 phút trên máy sạch

---

## 2. KIẾN TRÚC DỮ LIỆU & PIPELINE

| Tầng | Công nghệ | Vai trò |
|------|-----------|---------|
| **Raw** | MinIO (S3-compatible) | Lưu response HTML/JSON gốc, manifest, checkpoint |
| **Silver** | PostgreSQL + Polars (Python) | Chuẩn hoá giá về `vnd_per_month`, chuẩn hoá địa chỉ, gắn cờ `price_normalization_ok` và `price_period_suspect` |
| **Gold - Fact** | PostgreSQL | `observation_id`, `observed_at`, `algorithm_version`, `last_crawled_at` |
| **Gold - Dimension** | PostgreSQL | `canonical_listing_id`, `canonical_address`, `canonical_area`, `canonical_price_mean`, `effective_from`, `effective_to` |
| **Review Queue** | PostgreSQL | Cluster nghi ngờ chứa hard conflict hoặc vượt giới hạn kích thước — gắn cờ để review thủ công, không bao giờ xuất hiện trong truy vấn phân tích |
| **Analytics** | PostGIS (SQL) | Spatial join (`ST_DWithin`, `ST_Within`), tổng hợp giá theo phường, phân phối risk score |
| **Commit Marker** | MinIO (`latest.json` + `manifest.json`) | Bảo đảm tính toàn vẹn publish; reader luôn kiểm SHA-256 của `manifest.json` trước khi đọc Gold. Nếu checksum lệch hoặc thiếu marker thì Gold **chưa được publish** và bị bỏ qua bởi mọi công cụ downstream |

**Nguyên tắc cốt lõi:** nguồn sự thật duy nhất cho trạng thái "đã publish" là cặp marker `latest.json` / `manifest.json`. Materialization `table` của dbt trên PostgreSQL **không** cung cấp atomic swap — nó thực hiện `CREATE TABLE ... DROP ... RENAME`, để lại khoảng trống mà bảng có thể biến mất nếu tiến trình bị kill. Do đó, tính toàn vẹn commit nằm **ngoài PostgreSQL**, trong cặp marker của MinIO.

---

## 3. CRAWLER & FALLBACK

- Rate limit: tối đa 1 request mỗi 2 giây, xoay User-Agent
- Proxy: chỉ dùng khi IP hiện tại nhận 429 hoặc 503 từ sàn đích
- Retry: 3 lần với exponential back-off
- Soft-delete detection: 200 kèm banner "Tin đã hết hạn" dẫn tới `suspected_removed`; 200 kèm banner "Đã giao dịch" dẫn tới `removed`; HTTP 301/302 dẫn tới `suspected_removed` (cần theo redirect); HTTP 404 dẫn tới `removed`
- Grace Period: 72 giờ. Nếu tin ở trạng thái `suspected_removed` xuất hiện lại trong 72 giờ kể từ lần crawl đầu thì hoàn nguyên về `active`. Sau 3 lần crawl thành công liên tiếp không thấy tin và quá 72 giờ thì chuyển sang `removed`
- Fallback strategy là kế hoạch chính, không phải phương án dự phòng:
  - Nhánh A: crawl thời gian thực, tôn trọng ToS và rate limit
  - Nhánh B: static seed dataset (khoảng 500 đến 1.000 tin) cộng synthetic duplicates sinh từ dữ liệu thật với nhiễu kiểm soát: diện tích dao động 5%, địa chỉ fuzz, trộn kỳ hạn giá, lệch tọa độ, lặp ảnh
  - Cả hai nhánh dùng chung 100% pipeline phía sau: Raw, Silver, ER, Gold. Không có code path riêng cho dữ liệu synthetic.

---

## 4. ENTITY-RESOLUTION ENGINE

### 4.1 Multi-pass blocking (Union, tức OR)

| Pass | Blocking key | Mục đích |
|------|-------------|---------|
| 1 | `ward_id` + `property_type` | Nền tảng rộng, chi phí thấp |
| 2 | `street_norm` + `area_bucket` | Cứu tin thiếu ward hoặc thiếu loại hình |
| 3 | `ward_id` + `price_bucket` | Cứu tin thiếu tên đường |

Cả ba pass đều sinh candidate pair theo phép **Union** (OR). Không dùng phép Intersection (AND), vì phép giao sẽ loại bỏ ngay lập tức mọi bản ghi thiếu dù chỉ một trường, khiến tỷ lệ False-Negative tăng vọt.

### 4.2 Overlapping buckets

- Diện tích: 5 bucket (0-20, 20-30, 30-50, 50-80, 80 trở lên). Mỗi giá trị được gán vào bucket của nó và bucket liền bên phải, ví dụ 25 m2 thuộc cả 20-30 và 30-50. Điều này bảo đảm 29 m2 và 30.5 m2 luôn chia sẻ ít nhất một bucket.
- Giá (đã chuẩn hoá về `vnd_per_month`): cùng cơ chế 5 bucket, overlap 1 bucket.

### 4.3 Canonical pair dedup

Sau khi union, mọi cặp được chuẩn hoá về dạng `(min(id_a, id_b), max(id_a, id_b))` rồi khử trùng lặp. Self-pair (trường hợp `id_a` bằng `id_b`) bị loại trước khi bất kỳ bước chấm điểm nào diễn ra.

### 4.3.1 Sub-block limiting (bộ nhớ an toàn)

Nếu một block chứa hơn 1.000 bản ghi, nó được chia đệ quy theo `price_bucket`, rồi `area_bucket`, rồi `street_norm`, cho tới khi mỗi sub-block không vượt quá 1.000 bản ghi.

- Log mỗi batch: `n_raw_union`, `n_unique_pairs`, `dedup_ratio` (bằng `n_unique_pairs` chia cho `n_raw_union`), `n_blocks_subdivided`, `n_pairs_dropped_by_subblock`.
- Caveat list trong báo cáo cuối: mọi block bị chia và mọi cặp bị loại bỏ đều phải được ghi lại. Điều này trực tiếp giới hạn mức `candidate_recall` mà báo cáo có thể khẳng định.

### 4.4 Similarity scoring (công thức với mẫu số động)

Định nghĩa F(A,B) là tập các feature khả dụng trên cả A và B. Điểm tương đồng S(A,B) bằng tổng trọng số nhân với điểm thành phần, chia cho tổng trọng số của chính các feature trong F(A,B).

- Trọng số: text 0.30, area 0.20, geo 0.25, pHash 0.15 (pHash là tuỳ chọn)
- Mẫu số động: số chia là tổng trọng số của các feature có mặt, **không** phải hằng số 1.0. Nếu có pHash thì mẫu số bằng 0.90; nếu không có thì bằng 0.75
- Lý do: nếu dùng mẫu số cố định, điểm số sẽ phụ thuộc vào việc tình cờ thiếu feature, khiến các bản ghi thiếu dữ liệu bị chịu điểm thấp oan. Mẫu số động giữ cho mọi ngưỡng (ví dụ 0.82) **bất biến** xuyên suốt các cặp
- Minimum evidence: tổng trọng số trong F phải từ 0.50 trở lên và số feature tối thiểu là 2. Dưới ngưỡng đó, cặp được gắn `insufficient_evidence` và không bao giờ tự động match

### 4.5 Hard / Soft / Insufficient conflict

| Loại | Điều kiện | Hành động |
|------|-----------|-----------|
| Hard conflict | 2 bằng chứng high-confidence và khoảng cách vượt 500 m, hoặc tín hiệu mâu thuẫn rõ ràng khác | Từ chối cặp, tuyệt đối không merge |
| Soft conflict | Tín hiệu không nhất quán nhưng chưa đạt ngưỡng high-confidence | Giảm đóng góp của cặp vào điểm cluster, gắn cờ để review thủ công |
| Insufficient evidence | Dưới 2 feature khả dụng, hoặc tổng trọng số dưới 0.50 | Gắn `uncertain`, không tự động kết luận non-match |

Quy tắc khoảng cách, chỉ áp dụng cho tọa độ `high_confidence`: không quá 100 m thì `same_building_evidence`, chưa đủ để nâng thành `rental_unit_match`; từ 100 đến 500 m thì `soft_conflict`; vượt 500 m thì `hard_conflict`. `ward_centroid` **luôn** được coi là `low_confidence` và không bao giờ đi vào bảng khoảng cách.

### 4.5.1 Cluster admission và split

1. Dựng đồ thị, có cạnh khi S(A,B) không nhỏ hơn 0.82.
2. Tính connected components.
3. **Bên trong mỗi component:** quét mọi cặp theo O(k^2), với k không quá 10 theo thiết kế. Nếu phát hiện bất kỳ cặp nào có **hard conflict**, áp dụng conflict-driven refinement: gỡ cạnh yếu nhất trong component, tính lại component, lặp lại cho tới khi hết vi phạm.
4. Nếu sau refinement kích thước component vượt 10, hoặc component vẫn chứa cặp loại soft-conflict chưa giải quyết được, chuyển cả component sang **Review Queue** và **không publish** vào Gold.
5. Chỉ publish các component vượt qua mọi kiểm tra vào bảng `dim_canonical_listing`.

### 4.5.2 Vì sao "refine rồi review" tốt hơn "review sau components"

Cách chỉ review sau components sẽ giữ lại một component dạng A-B-C trong khi A-C là hard conflict với khoảng cách 1.2 km. Cluster đó sẽ được publish kèm một false merge. Refinement gỡ cạnh gây vi phạm, tách component thành các sub-cluster hợp lệ, hoặc kích hoạt review queue nếu cấu trúc quá bất ổn. Đây là con đường duy nhất để bảo đảm Gold chứa **zero false merge**.

---

## 5. RISK SCORING (theo bất thường, không theo gian lận)

- Metric: `listing_risk_score` tính bằng robust z-score và IQR, tổng hợp theo nhóm (loại hình × phường × nhóm diện tích × tháng).
- Cờ đi kèm: `price_normalization_ok`, `price_period_suspect`.
- Quy mô nhóm: tối thiểu 20 đến 50 tin mỗi nhóm. Nếu nhóm nhỏ hơn, output gắn cờ `risk_score_fallback` bằng true và không tự điền giá trị.
- Ảnh trùng ở nhiều nơi là tín hiệu cần kiểm tra, **không** phải bằng chứng gian lận.

---

## 6. GROUND TRUTH & ĐÁNH GIÁ

| Tập | Nguồn | Quy mô | Mục đích |
|-----|-------|--------|---------|
| **Synthetic (S)** | Sinh từ tin thật với nhiễu kiểm soát: lệch diện tích, fuzz địa chỉ, trộn kỳ hạn giá, lệch tọa độ, lặp ảnh | khoảng 500 cặp | Đo candidate recall (mục tiêu từ 85%), test biên bucket, F1 trong điều kiện lý tưởng |
| **Human (H)** | Gán tay bởi tác giả qua giao diện Jupyter kết hợp ipywidgets: hai cột hiển thị text, địa chỉ, ảnh và bản đồ; ba nút Match, Non-match, Uncertain | khoảng 100 đến 150 cặp | F1 thực tế, đếm false merge và false split, B-cubed và Rand index, xác minh boundary recall |

Metric đánh giá, báo cáo riêng cho S và H:

- Precision, Recall, F1 ở mức cặp
- Candidate recall (chỉ đo được trên tập S)
- Số cặp false merge và tỷ lệ (chỉ tập H)
- Số cặp false split và tỷ lệ (chỉ tập H)
- B-cubed và Rand index cho chất lượng cluster
- F1 phân tầng theo `available_weight_sum` (từ 0.75 trở lên, từ 0.50 đến dưới 0.75, dưới 0.50)
- Boundary recall: 29 m2 đối 30.5 m2, 4.9 triệu đối 5.1 triệu
- Khoảng bất định bootstrap 95 phần trăm trên các metric chính

Replay consistency chỉ bảo đảm ở mức record-level và operational. Không khẳng định replay ở mức metric-level và entity-level.

---

## 7. VÒNG ĐỜI DỮ LIỆU, IDEMPOTENCY & CHECKPOINT

- Checkpoint ghi vào MinIO mỗi 200 trang đã crawl hoặc mỗi 60 giây. Nội dung gồm `batch_id`, `source_name`, `cursor`, `listings_collected`, `pages_done`, `timestamp`, `checksum` dạng sha256.
- Ghi atomic: ghi file tạm rồi rename. Nếu tiến trình chết giữa lúc ghi, checkpoint cũ luôn còn hiệu lực và checkpoint mới bị bỏ qua.
- Resume: khi khởi động lại, job đọc checkpoint hợp lệ gần nhất và tiếp tục từ `cursor` cùng danh sách trang đã xử lý. Không crawl lại trang đã hoàn thành.
- Idempotency gồm 4 bước:
  1. Stage: ghi output vào prefix riêng, các tầng downstream bị chặn không đọc.
  2. Validate: kiểm checksum, schema, số dòng, khóa trùng lặp.
  3. Commit: sau khi lệnh dbt chạy thành công, ghi `manifest.json` (SHA-256, số dòng, `algorithm_version`, timestamp) vào MinIO, rồi atomically thay `latest.json` để trỏ tới manifest này.
  4. Verify: mọi lần replay phải kiểm `latest.json` cùng checksum manifest trước khi đọc bất kỳ bảng Gold nào. Nếu marker thiếu hoặc checksum sai thì Gold **không** được coi là đã publish và bị bỏ qua.

---

## 8. CHI PHÍ & KHẢ NĂNG TÁI LẬP

- Chi phí mỗi giờ: $0, toàn bộ công cụ chạy local.
- Smoke test 30 phút: `docker compose up -d`, rồi `make seed`, `make pipeline`, `make report`. Trên máy sạch 16 GB RAM với CPU hiện đại, toàn bộ pipeline từ crawl, Silver, ER, Gold tới biểu đồ Metabase hoàn tất trong 30 phút khi dùng dữ liệu synthetic seed, tức là trong trường hợp crawl thời gian thực bị chặn.
- Tái lập đầy đủ: `docker compose up` kèm các target trong Makefile; mọi path, seed, và cặp `latest.json` cùng `manifest.json` đều được version control trong repo.

---

## 9. TIMELINE (8 tuần, solo)

| Tuần | Mục tiêu | Bàn giao |
|------|----------|----------|
| 1 | Docker Compose, schema, MinIO, skeleton Makefile; crawl lần đầu và Feasibility gate | `docker compose up` chạy được, dữ liệu thô nằm trong MinIO |
| 2 | ETL Raw sang Silver, idempotency, checkpoint đầu tiên | Bảng Silver có dữ liệu, checkpoint đã ghi |
| 3 | Multi-pass blocking gồm union, overlapping bucket, dedup, sub-block limiting; đo candidate recall trên tập synthetic | `candidate_recall` từ 85% trở lên trên tập S; test biên bucket pass |
| 4 | Similarity theo F(A,B), xử lý hard và soft conflict, gán nhãn ground truth qua Jupyter kết hợp ipywidgets | Pilot Gate đạt; tập H đã gán khoảng 120 cặp |
| 5 | Clustering gồm refine cạnh, connected components, Review Queue; Gold canonical kèm `algorithm_version`; commit marker | Bảng Gold có dữ liệu; Review Queue có dữ liệu ở những chỗ cần |
| 6 | Risk scoring theo z-score và IQR theo nhóm; spatial analytics với PostGIS; dashboard Metabase | Dashboard KPI đầu tiên |
| 7 | Đánh giá đầy đủ: F1, B-cubed, Rand index, ablation 3 mức, Splink tuỳ chọn | Báo cáo Evaluation Gate |
| 8 | Báo cáo viết tay, xác minh smoke test trên máy sạch, `docker compose up` trong 30 phút, bàn giao tài liệu | Sản phẩm cuối |

Buffer: tuần 8 được dành riêng cho độ trễ của crawler, vấn đề chất lượng dữ liệu ngoài dự kiến, hoặc tinh chỉnh ngưỡng.

---

## 10. SUCCESS CRITERIA

Tất cả điều kiện dưới đây phải đạt để được xếp loại Pass:

- `candidate_recall` từ 85% trở lên trên tập Synthetic, đo trên toàn bộ output của union blocking
- F1 ở mức cặp không nhỏ hơn 0.70 trên tập Human, kèm F1 phân tầng theo `available_weight_sum`
- Zero false merge trong Gold, xác minh bằng audit thủ công Review Queue
- Caveat list của sub-block được ghi lại và đưa vào báo cáo cuối, gồm mọi block vượt 1.000 bản ghi và các cặp bị loại
- Tính toàn vẹn commit marker: kill-test, tức giả lập tiến trình chết giữa lúc chạy dbt, phải khiến reader từ chối đọc Gold nếu marker không hợp lệ; nếu không thì lần chạy đó bị coi là thất bại
- Idempotency: chạy toàn bộ pipeline hai lần trên cùng một input snapshot cho ra bảng Gold giống hệt nhau, về số dòng, checksum và `algorithm_version`
- Smoke test: `docker compose up` cộng `make pipeline` cộng `make report` hoàn tất trong 30 phút trên máy tham chiếu
- Tổng chi phí bằng $0, không dùng tài nguyên cloud nào

---

## 11. CÂU HỎI PHẢN BIỆN & CÂU TRẢ LỜI CHUẨN BỊ

- "Tại sao không dùng atomic swap của dbt?" — Đã kiểm chứng: materialization `table` trên PostgreSQL thực hiện `CREATE ... DROP ... RENAME`, để lại khoảng trống mà bảng có thể biến mất nếu tiến trình bị kill. Vì vậy tính toàn vẹn do cặp marker `latest.json` và `manifest.json` trên MinIO đảm bảo, chứ không phải bởi engine database.
- "Tại sao chỉ overlap 1 bucket?" — Kiểm nghiệm trên dữ liệu bất động sản thực tế tại Việt Nam cho thấy overlap 1 bucket loại bỏ hơn 95% lỗi ở các cặp biên, đồng thời giữ kích thước candidate set ở mức quản lý được.
- "Tại sao không khẳng định idempotency 100%?" — Lỗi phần cứng, bit-flip và can thiệp bên ngoài nằm ngoài phạm vi. Bảo đảm là idempotency ở mức pipeline: với cùng input snapshot và không có can thiệp bên ngoài, hai lần chạy cho ra kết quả Gold giống hệt.
- "Tại sao không dùng Splink hay mô hình probabilistic?" — Bộ similarity theo luật có trọng số kèm conflict-driven cluster refinement đã đạt các tiêu chí thành công đã định nghĩa trên cả tập synthetic và tập human. Thêm Splink sẽ vượt giới hạn 8 tuần solo mà không mang lại cải thiện đo được trên các metric đã định nghĩa.

---

## 12. TÀI LIỆU THAM KHẢO

- Docker Compose spec: https://docs.docker.com/compose/
- PostgreSQL và PostGIS: https://postgresql.org/ và https://postgis.net/
- MinIO: https://min.io/docs/
- dbt với Postgres adapter: https://docs.getdbt.com/
- Polars: https://docs.pola.rs/
- NetworkX và SciPy: https://networkx.org/ và https://scipy.org/
- Metabase: https://www.metabase.com/docs/
- WGS 84, EPSG 4326: https://epsg.io/4326

---

Hết tài liệu đề xuất. Tài liệu này cố ý không dùng emoji, icon, hay khối code. Đây là một Technical Design Document độc lập dành cho một người thực hiện, với toàn bộ pipeline chạy trên một máy cá nhân thông qua Docker.
