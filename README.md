# statistik-engine

Go microservice providing parallel statistics aggregation for Sekolah Pintar (port **8083**).

## Routes

All routes require a valid JWT Bearer token (`Authorization: Bearer <token>`).

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/statistik/overview` | KPI summary + 7-day sparklines |
| GET | `/api/v1/statistik/akademik` | Nilai distribution, top siswa, per-kelas |
| GET | `/api/v1/statistik/kehadiran` | Attendance trends + guru stats |
| GET | `/api/v1/statistik/keuangan` | SPP revenue, tunggakan per kelas |
| GET | `/api/v1/statistik/bk` | BK case trends, kategori, per-siswa |
| GET | `/api/v1/statistik/ppdb` | Funnel, gelombang breakdown, YoY |
| GET | `/api/v1/statistik/perpustakaan` | Book utilization, top buku |
| GET | `/api/v1/statistik/ujian` | Pass rate per mapel, nilai histogram |
| GET | `/api/v1/statistik/ekstrakurikuler` | Distribution per ekskul, per kelas |
| GET | `/api/v1/statistik/organisasi` | Distribution per org, jabatan |
| GET | `/api/v1/statistik/guru` | Guru kehadiran breakdown + trend |
| GET | `/api/v1/statistik/spk` | Skor distribution, top siswa |
| GET | `/health` | Health check (no auth) |

## Query Parameters

| Endpoint | Params |
|----------|--------|
| `akademik` | `kelas_id`, `tahun_ajaran_id` |
| `kehadiran` | `kelas_id`, `tahun_ajaran_id`, `bulan` (1-12), `tahun` |
| `keuangan` | `tahun` |
| `bk` | `tahun`, `kelas_id` |
| `ppdb` | `tahun` |
| `perpustakaan` | `tahun` |
| `ujian` | `kelas_id`, `mapel_id`, `semester`, `kkm` (default: 75) |
| `ekstrakurikuler` | `tahun` |
| `guru` | `start_date` (YYYY-MM-DD), `end_date` (YYYY-MM-DD) |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `APP_PORT` | `8083` | HTTP listen port |
| `JWT_SECRET` | required | Shared HMAC-SHA256 secret |
| `DB_HOST` | `127.0.0.1` | PostgreSQL host |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_DATABASE` | `db_sekolah` | Database name |
| `DB_USERNAME` | `root` | Database user |
| `DB_PASSWORD` | | Database password |

## Architecture

All statistics endpoints use `errgroup` for parallel fan-out across independent SQL queries.
This converts sequential $O(\sum t_i)$ query time to parallel $O(\max t_i)$.

```
Request → Auth Middleware → StatistikHandler
            → StatistikService
              → StatistikRepo.GetXxx(ctx)
                → errgroup fan-out → N parallel SQL queries → Merge → Response
```

## Running locally

```bash
cp src/.env .env
APP_PORT=8083 go run ./cmd/main.go
```

## Docker

```bash
# from /Users/bodo/www/sekolah
docker compose up statistik-engine
```
