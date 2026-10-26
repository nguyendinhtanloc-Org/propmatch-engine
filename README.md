# propmatch-engine

Hệ thống hợp nhất thực thể đa nguồn, chấm điểm rủi ro tin đăng và phân tích giá
rao bất động sản theo không gian.

Dự án solo, 8 tuần, chi phí hạ tầng $0. Toàn bộ pipeline chạy local dưới Docker.

## Bắt đầu nhanh

    make up        # khoi dong postgres/postgis, minio, dbt container
    make seed      # nap seed dataset va synthetic ground truth
    make pipeline  # raw -> silver -> er -> gold
    make report    # xuat KPI va ket qua danh gia ra reports/

Danh sách đầy đủ các target: `make help`

## Cấu trúc thư mục

    docs/proposal/   đề xuất đề tài (tiếng Việt, đặc tả kỹ thuật)
    docs/adr/        architecture decision records, mỗi file cho một quyết định
    src/propmatch/   mã nguồn pipeline, mỗi package một tầng
      common/        cấu hình, logging, hashing dùng chung
      ingest/        crawler, parser, bộ sinh synthetic seed
      silver/        chuẩn hoá: giá, địa chỉ, tọa độ
      er/            blocking, similarity, clustering
      risk/          chấm điểm bất thường
    dbt/models/      staging -> intermediate -> marts -> audit
    tests/           unit test và integration test
    notebooks/       exploration và gán nhãn thủ công (ipywidgets)
    data/            artifact local, đã git-ignore
    reports/         kết quả đánh giá được sinh ra
    infra/docker/    docker compose cho toàn bộ dịch vụ

## Tài liệu cần đọc trước

`docs/proposal/proposal_v3.1.md` là nguồn sự thật cho phạm vi, hợp đồng giữa các
tầng, thuật toán entity resolution và tiêu chí thành công. `docs/adr/` ghi lại
lý do của từng quyết định thiết kế không hiển nhiên.

## Yêu cầu

Docker và Docker Compose. Dependency Python sẽ được ghim phiên bản trong
`src/requirements.txt` khi viết mã nguồn pipeline.
