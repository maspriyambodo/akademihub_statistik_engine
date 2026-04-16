package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sekolahpintar/statistik-engine/internal/model"
	"golang.org/x/sync/errgroup"
)

type StatistikRepo struct {
	db *sqlx.DB
}

func NewStatistikRepo(db *sqlx.DB) *StatistikRepo {
	return &StatistikRepo{db: db}
}

// ─── helpers ──────────────────────────────────────────────────────────────

func namaBulan(m int) string {
	months := []string{"Jan", "Feb", "Mar", "Apr", "Mei", "Jun",
		"Jul", "Agu", "Sep", "Okt", "Nov", "Des"}
	if m < 1 || m > 12 {
		return ""
	}
	return months[m-1]
}

func generateColors(n int) []string {
	palette := []string{
		"#3B82F6", "#10B981", "#F59E0B", "#EF4444", "#8B5CF6",
		"#EC4899", "#06B6D4", "#F97316", "#84CC16", "#6366F1",
	}
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = palette[i%len(palette)]
	}
	return out
}

func round2(f float64) float64 {
	return float64(int(f*100+0.5)) / 100
}

func capitalizeFirst(s string) string {
	if s == "" {
		return s
	}
	return string(s[0]-32) + s[1:]
}

func countQuery(ctx context.Context, db *sqlx.DB, query string, args ...interface{}) (int64, error) {
	var n int64
	err := db.QueryRowContext(ctx, query, args...).Scan(&n)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return n, err
}

// ─── OVERVIEW ─────────────────────────────────────────────────────────────

func (r *StatistikRepo) GetOverview(ctx context.Context) (*model.OverviewResult, error) {
	today := time.Now().Format("2006-01-02")
	thisYear := time.Now().Year()
	thisMonth := int(time.Now().Month())

	// Fan-out: all counts in parallel
	var (
		totalSiswa    int64
		totalGuru     int64
		totalKelas    int64
		hadirHariIni  int64
		kasusBKProses int64
		pendaftarPPDB int64
		bukuDipinjam  int64
		hasilSPK      int64
	)

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		var err error
		totalSiswa, err = countQuery(gctx, r.db,
			`SELECT COUNT(*) FROM mst_siswa WHERE deleted_at IS NULL`)
		return err
	})
	g.Go(func() error {
		var err error
		totalGuru, err = countQuery(gctx, r.db,
			`SELECT COUNT(*) FROM mst_guru WHERE deleted_at IS NULL`)
		return err
	})
	g.Go(func() error {
		var err error
		totalKelas, err = countQuery(gctx, r.db,
			`SELECT COUNT(*) FROM mst_kelas WHERE deleted_at IS NULL`)
		return err
	})
	g.Go(func() error {
		var err error
		hadirHariIni, err = countQuery(gctx, r.db,
			`SELECT COUNT(*) FROM trx_absensi_siswa WHERE tanggal = $1 AND status = 1 AND deleted_at IS NULL`,
			today)
		return err
	})
	g.Go(func() error {
		var err error
		kasusBKProses, err = countQuery(gctx, r.db,
			`SELECT COUNT(*) FROM trx_bk_kasus WHERE status = 'proses' AND deleted_at IS NULL`)
		return err
	})
	g.Go(func() error {
		var err error
		pendaftarPPDB, err = countQuery(gctx, r.db,
			`SELECT COUNT(*) FROM ppdb_pendaftar WHERE EXTRACT(YEAR FROM created_at) = $1 AND deleted_at IS NULL`,
			thisYear)
		return err
	})
	g.Go(func() error {
		var err error
		bukuDipinjam, err = countQuery(gctx, r.db,
			`SELECT COUNT(*) FROM trx_peminjaman_buku WHERE status = 1`)
		return err
	})
	g.Go(func() error {
		var err error
		hasilSPK, err = countQuery(gctx, r.db,
			`SELECT COUNT(*) FROM spk_hasil WHERE deleted_at IS NULL`)
		return err
	})

	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("overview parallel query: %w", err)
	}

	// KPI Cards
	kpiCards := []model.KPICard{
		{
			ID: "siswa", Title: "Total Siswa", Value: fmt.Sprintf("%d", totalSiswa),
			Icon: "users", Color: "#2563EB", BgColor: "#EFF6FF",
			Trend: "neutral", TrendLabel: "Siswa aktif",
		},
		{
			ID: "guru", Title: "Total Guru", Value: fmt.Sprintf("%d", totalGuru),
			Icon: "graduation-cap", Color: "#059669", BgColor: "#ECFDF5",
			Trend: "neutral", TrendLabel: "Guru aktif",
		},
		{
			ID: "kelas", Title: "Total Kelas", Value: fmt.Sprintf("%d", totalKelas),
			Icon: "book-open", Color: "#7C3AED", BgColor: "#F5F3FF",
			Trend: "neutral", TrendLabel: "Kelas aktif",
		},
		{
			ID: "hadir", Title: "Hadir Hari Ini", Value: fmt.Sprintf("%d", hadirHariIni),
			Icon: "calendar", Color: "#16A34A", BgColor: "#F0FDF4",
			Trend: "up", TrendLabel: "Siswa hadir",
		},
		{
			ID: "bk", Title: "Kasus BK Proses", Value: fmt.Sprintf("%d", kasusBKProses),
			Icon: "shield", Color: "#DC2626", BgColor: "#FEF2F2",
			Trend: "down", TrendLabel: "Perlu penanganan",
		},
		{
			ID: "ppdb", Title: "Pendaftar PPDB", Value: fmt.Sprintf("%d", pendaftarPPDB),
			Icon: "user-plus", Color: "#D97706", BgColor: "#FFFBEB",
			Trend: "up", TrendLabel: fmt.Sprintf("Tahun %d", thisYear),
		},
		{
			ID: "perpus", Title: "Buku Dipinjam", Value: fmt.Sprintf("%d", bukuDipinjam),
			Icon: "library", Color: "#0891B2", BgColor: "#ECFEFF",
			Trend: "neutral", TrendLabel: "Sedang dipinjam",
		},
		{
			ID: "spk", Title: "Hasil SPK", Value: fmt.Sprintf("%d", hasilSPK),
			Icon: "award", Color: "#9333EA", BgColor: "#FAF5FF",
			Trend: "neutral", TrendLabel: "Evaluasi tersedia",
		},
	}

	// Sparkline: last 7 days kehadiran, SPP, BK
	sparkline, err := r.getSparkline7Days(ctx, thisMonth, thisYear)
	if err != nil {
		sparkline = &model.SparklineData{}
	}

	summary := model.OverviewSummary{
		TotalSiswa:    totalSiswa,
		TotalGuru:     totalGuru,
		TotalKelas:    totalKelas,
		HadirHariIni:  hadirHariIni,
		KasusBKProses: kasusBKProses,
		PendaftarPPDB: pendaftarPPDB,
		BukuDipinjam:  bukuDipinjam,
		HasilSPK:      hasilSPK,
	}

	return &model.OverviewResult{
		Summary:        summary,
		KPICards:       kpiCards,
		Sparkline7Days: *sparkline,
		Meta: map[string]string{
			"generated_at": time.Now().Format(time.RFC3339),
		},
	}, nil
}

func (r *StatistikRepo) getSparkline7Days(ctx context.Context, month, year int) (*model.SparklineData, error) {
	type dailyRow struct {
		Tanggal string `db:"tanggal"`
		Total   int64  `db:"total"`
	}

	kehadiranRows, err := r.db.QueryxContext(ctx, `
		SELECT tanggal::text, COUNT(*) as total
		FROM trx_absensi_siswa
		WHERE tanggal >= CURRENT_DATE - INTERVAL '6 days'
		  AND tanggal <= CURRENT_DATE
		  AND status = 1
		  AND deleted_at IS NULL
		GROUP BY tanggal
		ORDER BY tanggal
	`)
	if err != nil {
		return nil, err
	}
	defer kehadiranRows.Close()

	kehadiranMap := map[string]int64{}
	for kehadiranRows.Next() {
		var row dailyRow
		if err := kehadiranRows.StructScan(&row); err == nil {
			kehadiranMap[row.Tanggal] = row.Total
		}
	}

	bkRows, err := r.db.QueryxContext(ctx, `
		SELECT created_at::date::text as tanggal, COUNT(*) as total
		FROM trx_bk_kasus
		WHERE created_at >= CURRENT_DATE - INTERVAL '6 days'
		  AND created_at <= CURRENT_DATE + INTERVAL '1 day'
		  AND deleted_at IS NULL
		GROUP BY created_at::date
		ORDER BY created_at::date
	`)
	if err != nil {
		return nil, err
	}
	defer bkRows.Close()

	bkMap := map[string]int64{}
	for bkRows.Next() {
		var row dailyRow
		if err := bkRows.StructScan(&row); err == nil {
			bkMap[row.Tanggal] = row.Total
		}
	}

	sppRows, err := r.db.QueryxContext(ctx, `
		SELECT updated_at::date::text as tanggal, SUM(jumlah) as total
		FROM trx_pembayaran_spp
		WHERE updated_at >= CURRENT_DATE - INTERVAL '6 days'
		  AND updated_at <= CURRENT_DATE + INTERVAL '1 day'
		  AND status IN (SELECT code FROM sys_references WHERE group_name = 'status_bayar' AND name = 'Lunas' LIMIT 1)
		GROUP BY updated_at::date
		ORDER BY updated_at::date
	`)
	if err != nil {
		// fallback: skip SPP sparkline
		sppRows = nil
	}

	sppMap := map[string]int64{}
	if sppRows != nil {
		defer sppRows.Close()
		for sppRows.Next() {
			var row dailyRow
			if err := sppRows.StructScan(&row); err == nil {
				sppMap[row.Tanggal] = row.Total
			}
		}
	}

	labels := make([]string, 7)
	kehadiran := make([]int64, 7)
	spp := make([]int64, 7)
	bk := make([]int64, 7)

	for i := 6; i >= 0; i-- {
		d := time.Now().AddDate(0, 0, -i)
		key := d.Format("2006-01-02")
		idx := 6 - i
		labels[idx] = d.Format("02/01")
		kehadiran[idx] = kehadiranMap[key]
		spp[idx] = sppMap[key]
		bk[idx] = bkMap[key]
	}

	return &model.SparklineData{
		Labels:         labels,
		KehadiranSiswa: kehadiran,
		PendapatanSPP:  spp,
		KasusBK:        bk,
	}, nil
}

// ─── AKADEMIK ─────────────────────────────────────────────────────────────

func (r *StatistikRepo) GetAkademik(ctx context.Context, tahunAjaranID, kelasID *int64) (*model.AkademikResult, error) {
	var (
		totalSiswa    int64
		totalGuru     int64
		totalKelas    int64
		avgNilai      float64
	)

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		q := `SELECT COUNT(*) FROM mst_siswa WHERE deleted_at IS NULL`
		if kelasID != nil {
			q += fmt.Sprintf(` AND mst_kelas_id = %d`, *kelasID)
		}
		var err error
		totalSiswa, err = countQuery(gctx, r.db, q)
		return err
	})
	g.Go(func() error {
		var err error
		totalGuru, err = countQuery(gctx, r.db, `SELECT COUNT(*) FROM mst_guru WHERE deleted_at IS NULL`)
		return err
	})
	g.Go(func() error {
		var err error
		totalKelas, err = countQuery(gctx, r.db, `SELECT COUNT(*) FROM mst_kelas WHERE deleted_at IS NULL`)
		return err
	})
	g.Go(func() error {
		q := `SELECT COALESCE(AVG(nilai), 0) FROM trx_nilai WHERE deleted_at IS NULL`
		if kelasID != nil {
			q += fmt.Sprintf(` AND EXISTS (
				SELECT 1 FROM trx_ujian u
				WHERE u.id = trx_nilai.trx_ujian_id AND u.mst_kelas_id = %d
			)`, *kelasID)
		}
		return r.db.QueryRowContext(gctx, q).Scan(&avgNilai)
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Gender distribution
	type genderRow struct {
		JenisKelamin string `db:"jenis_kelamin"`
		Total        int64  `db:"total"`
	}
	genderQ := `
		SELECT COALESCE(jenis_kelamin, 'Tidak Diketahui') as jenis_kelamin, COUNT(*) as total
		FROM mst_siswa WHERE deleted_at IS NULL`
	if kelasID != nil {
		genderQ += fmt.Sprintf(` AND mst_kelas_id = %d`, *kelasID)
	}
	genderQ += ` GROUP BY jenis_kelamin`

	var genderRows []genderRow
	_ = r.db.SelectContext(ctx, &genderRows, genderQ)

	distribusiGender := model.ChartSeries{
		Colors: []string{"#3B82F6", "#EC4899", "#6B7280"},
	}
	for _, row := range genderRows {
		label := row.JenisKelamin
		if label == "L" {
			label = "Laki-laki"
		} else if label == "P" {
			label = "Perempuan"
		}
		distribusiGender.Labels = append(distribusiGender.Labels, label)
		distribusiGender.Data = append(distribusiGender.Data, row.Total)
	}

	// Distribusi per kelas
	type kelasRow struct {
		NamaKelas string `db:"nama_kelas"`
		Total     int64  `db:"total"`
	}
	var kelasRows []kelasRow
	_ = r.db.SelectContext(ctx, &kelasRows, `
		SELECT k.nama_kelas, COUNT(s.id) as total
		FROM mst_kelas k
		LEFT JOIN mst_siswa s ON s.mst_kelas_id = k.id AND s.deleted_at IS NULL
		WHERE k.deleted_at IS NULL
		GROUP BY k.id, k.nama_kelas
		ORDER BY k.nama_kelas
	`)

	distribusiKelas := model.ChartSeries{
		Colors: generateColors(len(kelasRows)),
	}
	for _, row := range kelasRows {
		distribusiKelas.Labels = append(distribusiKelas.Labels, row.NamaKelas)
		distribusiKelas.Data = append(distribusiKelas.Data, row.Total)
	}

	// Nilai histogram
	type histRow struct {
		Rentang string `db:"rentang"`
		Total   int64  `db:"total"`
	}
	histQ := `
		SELECT
			CASE
				WHEN nilai < 60 THEN 'Di bawah 60'
				WHEN nilai BETWEEN 60 AND 69 THEN '60 – 69'
				WHEN nilai BETWEEN 70 AND 79 THEN '70 – 79'
				WHEN nilai BETWEEN 80 AND 89 THEN '80 – 89'
				ELSE '90 – 100'
			END as rentang,
			COUNT(*) as total
		FROM trx_nilai WHERE deleted_at IS NULL`
	if kelasID != nil {
		histQ += fmt.Sprintf(` AND EXISTS (
			SELECT 1 FROM trx_ujian u WHERE u.id = trx_nilai.trx_ujian_id AND u.mst_kelas_id = %d
		)`, *kelasID)
	}
	histQ += ` GROUP BY rentang`

	var histRows []histRow
	_ = r.db.SelectContext(ctx, &histRows, histQ)

	histogramLabels := []string{"Di bawah 60", "60 – 69", "70 – 79", "80 – 89", "90 – 100"}
	histogramColors := []string{"#EF4444", "#F59E0B", "#FBBF24", "#34D399", "#10B981"}
	histMap := map[string]int64{}
	for _, h := range histRows {
		histMap[h.Rentang] = h.Total
	}
	histData := make([]int64, len(histogramLabels))
	for i, l := range histogramLabels {
		histData[i] = histMap[l]
	}

	histogram := model.NilaiHistogram{
		Labels: histogramLabels,
		Data:   histData,
		Colors: histogramColors,
	}

	// Top siswa by nilai
	topSiswaQ := `
		SELECT s.id, s.nama, s.nis, k.nama_kelas, ROUND(AVG(n.nilai)::numeric, 2) as rata_rata
		FROM trx_nilai n
		JOIN trx_ujian u ON u.id = n.trx_ujian_id
		JOIN mst_siswa s ON s.id = n.mst_siswa_id
		JOIN mst_kelas k ON k.id = s.mst_kelas_id
		WHERE n.deleted_at IS NULL`
	if kelasID != nil {
		topSiswaQ += fmt.Sprintf(` AND u.mst_kelas_id = %d`, *kelasID)
	}
	topSiswaQ += ` GROUP BY s.id, s.nama, s.nis, k.nama_kelas ORDER BY rata_rata DESC LIMIT 10`

	var topSiswa []model.TopSiswaItem
	_ = r.db.SelectContext(ctx, &topSiswa, topSiswaQ)

	// Rata-rata nilai per mapel
	type mapelRow struct {
		NamaMapel string  `db:"nama_mapel"`
		RataRata  float64 `db:"rata_rata"`
	}
	mapelQ := `
		SELECT m.nama_mapel, ROUND(AVG(n.nilai)::numeric, 2) as rata_rata
		FROM trx_nilai n
		JOIN trx_ujian u ON u.id = n.trx_ujian_id
		JOIN mst_mapel m ON m.id = u.mst_mapel_id
		WHERE n.deleted_at IS NULL`
	if kelasID != nil {
		mapelQ += fmt.Sprintf(` AND u.mst_kelas_id = %d`, *kelasID)
	}
	mapelQ += ` GROUP BY m.id, m.nama_mapel ORDER BY rata_rata DESC`

	var mapelRows []mapelRow
	_ = r.db.SelectContext(ctx, &mapelRows, mapelQ)

	nilaiPerMapel := model.ChartSeries{Colors: generateColors(len(mapelRows))}
	for _, m := range mapelRows {
		nilaiPerMapel.Labels = append(nilaiPerMapel.Labels, m.NamaMapel)
		nilaiPerMapel.Data = append(nilaiPerMapel.Data, int64(m.RataRata))
	}

	return &model.AkademikResult{
		Summary: model.AkademikSummary{
			TotalSiswa:    totalSiswa,
			TotalGuru:     totalGuru,
			TotalKelas:    totalKelas,
			RataRataNilai: round2(avgNilai),
		},
		DistribusiGender: distribusiGender,
		DistribusiKelas:  distribusiKelas,
		NilaiHistogram:   histogram,
		TopSiswa:         topSiswa,
		NilaiPerMapel:    nilaiPerMapel,
	}, nil
}

// ─── KEHADIRAN ────────────────────────────────────────────────────────────

func (r *StatistikRepo) GetKehadiran(ctx context.Context, tahunAjaranID, kelasID *int64, bulan, tahun int) (*model.KehadiranResult, error) {
	var (
		totalHadir  int64
		totalIzin   int64
		totalSakit  int64
		totalAlpha  int64
		guruHadir   int64
		guruTotal   int64
	)

	baseWhere := fmt.Sprintf(`EXTRACT(YEAR FROM tanggal) = %d AND deleted_at IS NULL`, tahun)
	if bulan > 0 {
		baseWhere += fmt.Sprintf(` AND EXTRACT(MONTH FROM tanggal) = %d`, bulan)
	}
	if kelasID != nil {
		baseWhere += fmt.Sprintf(` AND mst_kelas_id = %d`, *kelasID)
	}

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		var err error
		totalHadir, err = countQuery(gctx, r.db,
			fmt.Sprintf(`SELECT COUNT(*) FROM trx_absensi_siswa WHERE %s AND status = 1`, baseWhere))
		return err
	})
	g.Go(func() error {
		var err error
		totalIzin, err = countQuery(gctx, r.db,
			fmt.Sprintf(`SELECT COUNT(*) FROM trx_absensi_siswa WHERE %s AND status = 3`, baseWhere))
		return err
	})
	g.Go(func() error {
		var err error
		totalSakit, err = countQuery(gctx, r.db,
			fmt.Sprintf(`SELECT COUNT(*) FROM trx_absensi_siswa WHERE %s AND status = 2`, baseWhere))
		return err
	})
	g.Go(func() error {
		var err error
		totalAlpha, err = countQuery(gctx, r.db,
			fmt.Sprintf(`SELECT COUNT(*) FROM trx_absensi_siswa WHERE %s AND status = 4`, baseWhere))
		return err
	})

	guruWhere := fmt.Sprintf(`EXTRACT(YEAR FROM tanggal) = %d AND deleted_at IS NULL`, tahun)
	if bulan > 0 {
		guruWhere += fmt.Sprintf(` AND EXTRACT(MONTH FROM tanggal) = %d`, bulan)
	}

	g.Go(func() error {
		var err error
		guruHadir, err = countQuery(gctx, r.db,
			fmt.Sprintf(`SELECT COUNT(*) FROM trx_absensi_guru WHERE %s AND status IS NOT NULL`, guruWhere))
		_ = err
		// Count hadir guru (any check-in record = present)
		guruHadir, err = countQuery(gctx, r.db,
			fmt.Sprintf(`SELECT COUNT(*) FROM trx_absensi_guru WHERE %s AND jam_masuk IS NOT NULL`, guruWhere))
		return err
	})
	g.Go(func() error {
		var err error
		guruTotal, err = countQuery(gctx, r.db,
			fmt.Sprintf(`SELECT COUNT(*) FROM trx_absensi_guru WHERE %s`, guruWhere))
		return err
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	total := totalHadir + totalIzin + totalSakit + totalAlpha
	var pctHadir float64
	if total > 0 {
		pctHadir = round2(float64(totalHadir) / float64(total) * 100)
	}

	var guruPct float64
	if guruTotal > 0 {
		guruPct = round2(float64(guruHadir) / float64(guruTotal) * 100)
	}

	// Tren bulanan (12 months)
	type monthRow struct {
		Bulan  int   `db:"bulan"`
		Hadir  int64 `db:"hadir"`
		Izin   int64 `db:"izin"`
		Sakit  int64 `db:"sakit"`
		Alpha  int64 `db:"alpha"`
	}
	var monthRows []monthRow
	_ = r.db.SelectContext(ctx, &monthRows, fmt.Sprintf(`
		SELECT
			EXTRACT(MONTH FROM tanggal)::int as bulan,
			SUM(CASE WHEN status = 1 THEN 1 ELSE 0 END) as hadir,
			SUM(CASE WHEN status = 3 THEN 1 ELSE 0 END) as izin,
			SUM(CASE WHEN status = 2 THEN 1 ELSE 0 END) as sakit,
			SUM(CASE WHEN status = 4 THEN 1 ELSE 0 END) as alpha
		FROM trx_absensi_siswa
		WHERE EXTRACT(YEAR FROM tanggal) = %d AND deleted_at IS NULL %s
		GROUP BY EXTRACT(MONTH FROM tanggal)::int
		ORDER BY bulan
	`, tahun, func() string {
		if kelasID != nil {
			return fmt.Sprintf(` AND mst_kelas_id = %d`, *kelasID)
		}
		return ""
	}()))

	monthMap := map[int]monthRow{}
	for _, row := range monthRows {
		monthMap[row.Bulan] = row
	}

	labels := make([]string, 12)
	hadir := make([]int64, 12)
	izin := make([]int64, 12)
	sakit := make([]int64, 12)
	alpha := make([]int64, 12)
	for m := 1; m <= 12; m++ {
		labels[m-1] = namaBulan(m)
		row := monthMap[m]
		hadir[m-1] = row.Hadir
		izin[m-1] = row.Izin
		sakit[m-1] = row.Sakit
		alpha[m-1] = row.Alpha
	}

	trenBulanan := model.MultiSeriesChart{
		Labels: labels,
		Datasets: []model.DatasetItem{
			{Label: "Hadir", Data: hadir, Color: "#10B981"},
			{Label: "Izin", Data: izin, Color: "#3B82F6"},
			{Label: "Sakit", Data: sakit, Color: "#F59E0B"},
			{Label: "Alpha", Data: alpha, Color: "#EF4444"},
		},
	}

	// Per kelas
	type kelasRow struct {
		NamaKelas string `db:"nama_kelas"`
		Total     int64  `db:"total"`
	}
	var perKelas []kelasRow
	_ = r.db.SelectContext(ctx, &perKelas, fmt.Sprintf(`
		SELECT k.nama_kelas, COUNT(a.id) as total
		FROM mst_kelas k
		LEFT JOIN trx_absensi_siswa a ON a.mst_kelas_id = k.id
			AND EXTRACT(YEAR FROM a.tanggal) = %d
			AND a.status = 1 AND a.deleted_at IS NULL
		WHERE k.deleted_at IS NULL
		GROUP BY k.id, k.nama_kelas
		ORDER BY total DESC
		LIMIT 15
	`, tahun))

	distribusiKelas := model.ChartSeries{Colors: generateColors(len(perKelas))}
	for _, k := range perKelas {
		distribusiKelas.Labels = append(distribusiKelas.Labels, k.NamaKelas)
		distribusiKelas.Data = append(distribusiKelas.Data, k.Total)
	}

	return &model.KehadiranResult{
		Summary: model.KehadiranSummary{
			TotalHadir:      totalHadir,
			TotalIzin:       totalIzin,
			TotalSakit:      totalSakit,
			TotalAlpha:      totalAlpha,
			PersentaseHadir: pctHadir,
		},
		TrenBulanan: trenBulanan,
		StatusDonut: model.ChartSeries{
			Labels: []string{"Hadir", "Izin", "Sakit", "Alpha"},
			Data:   []int64{totalHadir, totalIzin, totalSakit, totalAlpha},
			Colors: []string{"#10B981", "#3B82F6", "#F59E0B", "#EF4444"},
		},
		PerKelas: distribusiKelas,
		GuruKehadiran: model.GuruKehadiran{
			TotalHadir:      guruHadir,
			TotalAbsensi:    guruTotal,
			PersentaseHadir: guruPct,
		},
	}, nil
}

// ─── KEUANGAN ─────────────────────────────────────────────────────────────

func (r *StatistikRepo) GetKeuangan(ctx context.Context, tahun int) (*model.KeuanganResult, error) {
	// Get lunas reference code
	var lunasCode string
	_ = r.db.QueryRowContext(ctx, `
		SELECT code FROM sys_references
		WHERE group_name = 'status_bayar' AND name = 'Lunas'
		LIMIT 1
	`).Scan(&lunasCode)
	if lunasCode == "" {
		lunasCode = "1" // fallback
	}

	var totalPendapatan, totalLunas, totalBelumLunas int64

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return r.db.QueryRowContext(gctx, fmt.Sprintf(`
			SELECT COALESCE(SUM(jumlah), 0)
			FROM trx_pembayaran_spp
			WHERE EXTRACT(YEAR FROM created_at) = %d AND status = '%s'
		`, tahun, lunasCode)).Scan(&totalPendapatan)
	})
	g.Go(func() error {
		var err error
		totalLunas, err = countQuery(gctx, r.db, fmt.Sprintf(`
			SELECT COUNT(*) FROM trx_pembayaran_spp
			WHERE EXTRACT(YEAR FROM created_at) = %d AND status = '%s'
		`, tahun, lunasCode))
		return err
	})
	g.Go(func() error {
		var err error
		totalBelumLunas, err = countQuery(gctx, r.db, fmt.Sprintf(`
			SELECT COUNT(*) FROM trx_pembayaran_spp
			WHERE EXTRACT(YEAR FROM created_at) = %d AND status != '%s'
		`, tahun, lunasCode))
		return err
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	total := totalLunas + totalBelumLunas
	var pctLunas float64
	if total > 0 {
		pctLunas = round2(float64(totalLunas) / float64(total) * 100)
	}

	// Tren bulanan
	type monthRow struct {
		Bulan int   `db:"bulan"`
		Total int64 `db:"total"`
	}
	var trenRows []monthRow
	_ = r.db.SelectContext(ctx, &trenRows, fmt.Sprintf(`
		SELECT EXTRACT(MONTH FROM created_at)::int as bulan, COALESCE(SUM(jumlah), 0) as total
		FROM trx_pembayaran_spp
		WHERE EXTRACT(YEAR FROM created_at) = %d AND status = '%s'
		GROUP BY EXTRACT(MONTH FROM created_at)::int
		ORDER BY bulan
	`, tahun, lunasCode))

	trenMap := map[int]int64{}
	for _, row := range trenRows {
		trenMap[row.Bulan] = row.Total
	}

	trenLabels := make([]string, 12)
	trenData := make([]int64, 12)
	for m := 1; m <= 12; m++ {
		trenLabels[m-1] = namaBulan(m)
		trenData[m-1] = trenMap[m]
	}

	// Tunggakan per kelas
	var tunggakanRows []model.TunggakanKelas
	_ = r.db.SelectContext(ctx, &tunggakanRows, fmt.Sprintf(`
		SELECT k.nama_kelas, COUNT(p.id) as total_tunggak
		FROM trx_pembayaran_spp p
		JOIN mst_siswa s ON s.id = p.mst_siswa_id
		JOIN mst_kelas k ON k.id = s.mst_kelas_id
		WHERE EXTRACT(YEAR FROM p.created_at) = %d AND p.status != '%s'
		GROUP BY k.id, k.nama_kelas
		ORDER BY total_tunggak DESC
		LIMIT 10
	`, tahun, lunasCode))

	return &model.KeuanganResult{
		Summary: model.KeuanganSummary{
			TotalPendapatan: totalPendapatan,
			TotalLunas:      totalLunas,
			TotalBelumLunas: totalBelumLunas,
			PersentaseLunas: pctLunas,
		},
		TrenBulanan: model.ChartSeries{
			Labels: trenLabels,
			Data:   trenData,
			Color:  "#10B981",
		},
		StatusDistribusi: model.ChartSeries{
			Labels: []string{"Lunas", "Belum Lunas"},
			Data:   []int64{totalLunas, totalBelumLunas},
			Colors: []string{"#10B981", "#EF4444"},
		},
		TunggakanPerKelas: tunggakanRows,
	}, nil
}

// ─── BK ───────────────────────────────────────────────────────────────────

func (r *StatistikRepo) GetBK(ctx context.Context, tahun int, kelasID *int64) (*model.BKResult, error) {
	baseWhere := fmt.Sprintf(`EXTRACT(YEAR FROM trx_bk_kasus.created_at) = %d AND trx_bk_kasus.deleted_at IS NULL`, tahun)
	if kelasID != nil {
		baseWhere += fmt.Sprintf(` AND trx_bk_kasus.mst_siswa_id IN (
			SELECT id FROM mst_siswa WHERE mst_kelas_id = %d AND deleted_at IS NULL
		)`, *kelasID)
	}

	var (
		totalKasus   int64
		kasusProses  int64
		kasusSelesai int64
		avgResolusi  float64
	)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		var err error
		totalKasus, err = countQuery(gctx, r.db, fmt.Sprintf(`SELECT COUNT(*) FROM trx_bk_kasus WHERE %s`, baseWhere))
		return err
	})
	g.Go(func() error {
		var err error
		kasusProses, err = countQuery(gctx, r.db, fmt.Sprintf(`SELECT COUNT(*) FROM trx_bk_kasus WHERE %s AND status = 'proses'`, baseWhere))
		return err
	})
	g.Go(func() error {
		var err error
		kasusSelesai, err = countQuery(gctx, r.db, fmt.Sprintf(`SELECT COUNT(*) FROM trx_bk_kasus WHERE %s AND status = 'selesai'`, baseWhere))
		return err
	})
	g.Go(func() error {
		return r.db.QueryRowContext(gctx, fmt.Sprintf(`
			SELECT COALESCE(AVG(tanggal_selesai::date - tanggal_mulai::date), 0)
			FROM trx_bk_kasus
			WHERE %s AND tanggal_selesai IS NOT NULL AND tanggal_mulai IS NOT NULL
		`, baseWhere)).Scan(&avgResolusi)
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	var resolusiRate float64
	if totalKasus > 0 {
		resolusiRate = round2(float64(kasusSelesai) / float64(totalKasus) * 100)
	}

	// Tren per bulan
	type monthRow struct {
		Bulan int   `db:"bulan"`
		Total int64 `db:"total"`
	}
	var trenRows []monthRow
	_ = r.db.SelectContext(ctx, &trenRows, fmt.Sprintf(`
		SELECT EXTRACT(MONTH FROM created_at)::int as bulan, COUNT(*) as total
		FROM trx_bk_kasus
		WHERE %s
		GROUP BY EXTRACT(MONTH FROM created_at)::int
		ORDER BY bulan
	`, baseWhere))

	trenMap := map[int]int64{}
	for _, row := range trenRows {
		trenMap[row.Bulan] = row.Total
	}
	trenLabels := make([]string, 12)
	trenData := make([]int64, 12)
	for m := 1; m <= 12; m++ {
		trenLabels[m-1] = namaBulan(m)
		trenData[m-1] = trenMap[m]
	}

	// Kategori distribution
	type labelTotal struct {
		Nama  string `db:"nama"`
		Total int64  `db:"total"`
	}
	var perKategori []labelTotal
	_ = r.db.SelectContext(ctx, &perKategori, fmt.Sprintf(`
		SELECT mk.nama, COUNT(*) as total
		FROM trx_bk_kasus bk
		JOIN mst_bk_kategori mk ON mk.id = bk.mst_bk_kategori_id
		WHERE %s
		GROUP BY mk.id, mk.nama
		ORDER BY total DESC
	`, baseWhere))

	distribusiKategori := model.ChartSeries{Colors: generateColors(len(perKategori))}
	for _, k := range perKategori {
		distribusiKategori.Labels = append(distribusiKategori.Labels, k.Nama)
		distribusiKategori.Data = append(distribusiKategori.Data, k.Total)
	}

	// Jenis distribution
	var perJenis []labelTotal
	_ = r.db.SelectContext(ctx, &perJenis, fmt.Sprintf(`
		SELECT mj.nama, COUNT(*) as total
		FROM trx_bk_kasus bk
		JOIN mst_bk_jenis mj ON mj.id = bk.mst_bk_jenis_id
		WHERE %s
		GROUP BY mj.id, mj.nama
		ORDER BY total DESC
	`, baseWhere))

	distribusiJenis := model.ChartSeries{Colors: generateColors(len(perJenis))}
	for _, j := range perJenis {
		distribusiJenis.Labels = append(distribusiJenis.Labels, j.Nama)
		distribusiJenis.Data = append(distribusiJenis.Data, j.Total)
	}

	// Per kelas
	var perKelas []labelTotal
	_ = r.db.SelectContext(ctx, &perKelas, fmt.Sprintf(`
		SELECT k.nama_kelas as nama, COUNT(bk.id) as total
		FROM trx_bk_kasus bk
		JOIN mst_siswa s ON s.id = bk.mst_siswa_id
		JOIN mst_kelas k ON k.id = s.mst_kelas_id
		WHERE %s
		GROUP BY k.id, k.nama_kelas
		ORDER BY total DESC
	`, baseWhere))

	distribusiKelas := model.ChartSeries{Colors: generateColors(len(perKelas))}
	for _, k := range perKelas {
		distribusiKelas.Labels = append(distribusiKelas.Labels, k.Nama)
		distribusiKelas.Data = append(distribusiKelas.Data, k.Total)
	}

	// Top siswa by kasus
	var siswaItems []model.SiswaBKItem
	_ = r.db.SelectContext(ctx, &siswaItems, fmt.Sprintf(`
		SELECT s.id, s.nama, s.nis, k.nama_kelas, COUNT(bk.id) as total_kasus
		FROM trx_bk_kasus bk
		JOIN mst_siswa s ON s.id = bk.mst_siswa_id
		JOIN mst_kelas k ON k.id = s.mst_kelas_id
		WHERE %s
		GROUP BY s.id, s.nama, s.nis, k.nama_kelas
		ORDER BY total_kasus DESC
		LIMIT 10
	`, baseWhere))

	return &model.BKResult{
		Summary: model.BKSummary{
			TotalKasus:      totalKasus,
			KasusProses:     kasusProses,
			KasusSelesai:    kasusSelesai,
			ResolusiRate:    resolusiRate,
			AvgResolusiHari: round2(avgResolusi),
		},
		TrenKasusBulanan: model.ChartSeries{
			Labels: trenLabels,
			Data:   trenData,
			Color:  "#EF4444",
		},
		StatusDistribution: model.ChartSeries{
			Labels: []string{"Proses", "Selesai"},
			Data:   []int64{kasusProses, kasusSelesai},
			Colors: []string{"#F59E0B", "#10B981"},
		},
		DistribusiKategori: distribusiKategori,
		DistribusiJenis:    distribusiJenis,
		DistribusiPerKelas: distribusiKelas,
		SiswaCasingTerbanyak: siswaItems,
		Tahun: tahun,
	}, nil
}

// ─── PPDB ─────────────────────────────────────────────────────────────────

func (r *StatistikRepo) GetPPDB(ctx context.Context, tahun int) (*model.PPDBResult, error) {
	stages := []string{"draft", "terverifikasi", "seleksi", "diterima", "cadangan", "ditolak"}
	stageColors := []string{"#6B7280", "#3B82F6", "#F59E0B", "#10B981", "#EC4899", "#EF4444"}

	type stageRow struct {
		Status string `db:"status_pendaftaran"`
		Total  int64  `db:"total"`
	}
	var stageRows []stageRow
	_ = r.db.SelectContext(ctx, &stageRows, fmt.Sprintf(`
		SELECT status_pendaftaran, COUNT(*) as total
		FROM ppdb_pendaftar
		WHERE EXTRACT(YEAR FROM created_at) = %d AND deleted_at IS NULL
		GROUP BY status_pendaftaran
	`, tahun))

	stageMap := map[string]int64{}
	for _, s := range stageRows {
		stageMap[s.Status] = s.Total
	}

	var funnel []model.FunnelItem
	var totalPendaftar, totalDiterima, totalDitolak int64
	for i, stage := range stages {
		total := stageMap[stage]
		totalPendaftar += total
		if stage == "diterima" {
			totalDiterima = total
		}
		if stage == "ditolak" {
			totalDitolak = total
		}
		funnel = append(funnel, model.FunnelItem{
			Stage: capitalizeFirst(stage),
			Total: total,
			Color: stageColors[i],
		})
	}

	var acceptanceRate float64
	if totalPendaftar > 0 {
		acceptanceRate = round2(float64(totalDiterima) / float64(totalPendaftar) * 100)
	}

	// Tren pendaftar per bulan
	type monthRow struct {
		Bulan int   `db:"bulan"`
		Total int64 `db:"total"`
	}
	var trenRows []monthRow
	_ = r.db.SelectContext(ctx, &trenRows, fmt.Sprintf(`
		SELECT EXTRACT(MONTH FROM created_at)::int as bulan, COUNT(*) as total
		FROM ppdb_pendaftar
		WHERE EXTRACT(YEAR FROM created_at) = %d AND deleted_at IS NULL
		GROUP BY EXTRACT(MONTH FROM created_at)::int
		ORDER BY bulan
	`, tahun))

	trenMap := map[int]int64{}
	for _, t := range trenRows {
		trenMap[t.Bulan] = t.Total
	}
	trenLabels := make([]string, 12)
	trenData := make([]int64, 12)
	for m := 1; m <= 12; m++ {
		trenLabels[m-1] = namaBulan(m)
		trenData[m-1] = trenMap[m]
	}

	// Per gelombang
	type gelombangRow struct {
		NamaGelombang  string  `db:"nama_gelombang"`
		TotalPendaftar int64   `db:"total_pendaftar"`
		TotalDiterima  int64   `db:"total_diterima"`
	}
	var gelombangRows []gelombangRow
	_ = r.db.SelectContext(ctx, &gelombangRows, fmt.Sprintf(`
		SELECT g.nama_gelombang,
			COUNT(p.id) as total_pendaftar,
			SUM(CASE WHEN p.status_pendaftaran = 'diterima' THEN 1 ELSE 0 END) as total_diterima
		FROM ppdb_pendaftar p
		JOIN ppdb_gelombang g ON g.id = p.ppdb_gelombang_id
		WHERE EXTRACT(YEAR FROM p.created_at) = %d AND p.deleted_at IS NULL
		GROUP BY g.id, g.nama_gelombang
		ORDER BY g.nama_gelombang
	`, tahun))

	gelombangLabels := make([]string, len(gelombangRows))
	gelombangPendaftar := make([]int64, len(gelombangRows))
	gelombangDiterima := make([]int64, len(gelombangRows))
	gelombangRate := make([]float64, len(gelombangRows))
	for i, g := range gelombangRows {
		gelombangLabels[i] = g.NamaGelombang
		gelombangPendaftar[i] = g.TotalPendaftar
		gelombangDiterima[i] = g.TotalDiterima
		if g.TotalPendaftar > 0 {
			gelombangRate[i] = round2(float64(g.TotalDiterima) / float64(g.TotalPendaftar) * 100)
		}
	}

	// YoY
	var prevTotal, prevDiterima int64
	_ = r.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM ppdb_pendaftar WHERE EXTRACT(YEAR FROM created_at) = %d AND deleted_at IS NULL`, tahun-1)).Scan(&prevTotal)
	_ = r.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM ppdb_pendaftar WHERE EXTRACT(YEAR FROM created_at) = %d AND status_pendaftaran = 'diterima' AND deleted_at IS NULL`, tahun-1)).Scan(&prevDiterima)

	var yoy float64
	if prevTotal > 0 {
		yoy = round2(float64(totalPendaftar-prevTotal) / float64(prevTotal) * 100)
	}

	return &model.PPDBResult{
		Summary: model.PPDBSummary{
			TotalPendaftar:   totalPendaftar,
			TotalDiterima:    totalDiterima,
			TotalDitolak:     totalDitolak,
			AcceptanceRate:   acceptanceRate,
			YoYGrowthPct:     yoy,
			PrevYearTotal:    prevTotal,
			PrevYearDiterima: prevDiterima,
		},
		Funnel: funnel,
		TrenPendaftaran: model.ChartSeries{
			Labels: trenLabels,
			Data:   trenData,
			Color:  "#EC4899",
		},
		DistribusiGelombang: model.DistribusiGelombang{
			Labels:         gelombangLabels,
			TotalPendaftar: gelombangPendaftar,
			TotalDiterima:  gelombangDiterima,
			AcceptanceRate: gelombangRate,
			Colors:         generateColors(len(gelombangRows)),
		},
		Tahun: tahun,
	}, nil
}

// ─── PERPUSTAKAAN ─────────────────────────────────────────────────────────

func (r *StatistikRepo) GetPerpustakaan(ctx context.Context, tahun int) (*model.PerpustakaanResult, error) {
	var totalBuku, totalAktif, totalOverdue int64

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		var err error
		totalBuku, err = countQuery(gctx, r.db, `SELECT COUNT(*) FROM mst_buku WHERE deleted_at IS NULL`)
		return err
	})
	g.Go(func() error {
		var err error
		totalAktif, err = countQuery(gctx, r.db, `SELECT COUNT(*) FROM trx_peminjaman_buku WHERE status = 1`)
		return err
	})
	g.Go(func() error {
		var err error
		totalOverdue, err = countQuery(gctx, r.db, `
			SELECT COUNT(*) FROM trx_peminjaman_buku
			WHERE status = 1
			  AND tanggal_jatuh_tempo IS NOT NULL
			  AND tanggal_jatuh_tempo < CURRENT_DATE
		`)
		return err
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	var utilizationRate float64
	if totalBuku > 0 {
		utilizationRate = round2(float64(totalAktif) / float64(totalBuku) * 100)
	}

	// Tren pinjam & kembali
	type monthRow struct {
		Bulan int   `db:"bulan"`
		Total int64 `db:"total"`
	}
	var trenPinjamRows, trenKembaliRows []monthRow
	_ = r.db.SelectContext(ctx, &trenPinjamRows, fmt.Sprintf(`
		SELECT EXTRACT(MONTH FROM tanggal_pinjam)::int as bulan, COUNT(*) as total
		FROM trx_peminjaman_buku
		WHERE EXTRACT(YEAR FROM tanggal_pinjam) = %d
		GROUP BY EXTRACT(MONTH FROM tanggal_pinjam)::int
		ORDER BY bulan
	`, tahun))
	_ = r.db.SelectContext(ctx, &trenKembaliRows, fmt.Sprintf(`
		SELECT EXTRACT(MONTH FROM tanggal_kembali)::int as bulan, COUNT(*) as total
		FROM trx_peminjaman_buku
		WHERE EXTRACT(YEAR FROM tanggal_kembali) = %d AND tanggal_kembali IS NOT NULL
		GROUP BY EXTRACT(MONTH FROM tanggal_kembali)::int
		ORDER BY bulan
	`, tahun))

	pinjamMap := map[int]int64{}
	for _, row := range trenPinjamRows {
		pinjamMap[row.Bulan] = row.Total
	}
	kembaliMap := map[int]int64{}
	for _, row := range trenKembaliRows {
		kembaliMap[row.Bulan] = row.Total
	}

	labels := make([]string, 12)
	pinjam := make([]int64, 12)
	kembali := make([]int64, 12)
	for m := 1; m <= 12; m++ {
		labels[m-1] = namaBulan(m)
		pinjam[m-1] = pinjamMap[m]
		kembali[m-1] = kembaliMap[m]
	}

	// Top buku
	var topBuku []model.BukuItem
	_ = r.db.SelectContext(ctx, &topBuku, `
		SELECT b.judul, b.penulis, COUNT(p.id) as total_dipinjam
		FROM trx_peminjaman_buku p
		JOIN mst_buku b ON b.id = p.mst_buku_id
		GROUP BY b.id, b.judul, b.penulis
		ORDER BY total_dipinjam DESC
		LIMIT 10
	`)

	// Siswa aktif pinjam
	var siswaAktif []model.SiswaPinjamItem
	_ = r.db.SelectContext(ctx, &siswaAktif, `
		SELECT s.nama, s.nis, k.nama_kelas, COUNT(p.id) as total_pinjam
		FROM trx_peminjaman_buku p
		JOIN mst_siswa s ON s.id = p.mst_siswa_id
		JOIN mst_kelas k ON k.id = s.mst_kelas_id
		GROUP BY s.id, s.nama, s.nis, k.nama_kelas
		ORDER BY total_pinjam DESC
		LIMIT 10
	`)

	return &model.PerpustakaanResult{
		Summary: model.PerpustakaanSummary{
			TotalJudulBuku:  totalBuku,
			SedangDipinjam:  totalAktif,
			Overdue:         totalOverdue,
			UtilizationRate: utilizationRate,
		},
		TrenPeminjaman: model.MultiSeriesChart{
			Labels: labels,
			Datasets: []model.DatasetItem{
				{Label: "Dipinjam", Data: pinjam, Color: "#6366F1"},
				{Label: "Dikembalikan", Data: kembali, Color: "#10B981"},
			},
		},
		TopBukuDiminati:  topBuku,
		SiswaAktifPinjam: siswaAktif,
		Tahun:            tahun,
	}, nil
}

// ─── UJIAN ────────────────────────────────────────────────────────────────

func (r *StatistikRepo) GetUjian(ctx context.Context, kelasID, mapelID *int64, semester string, kkm float64) (*model.UjianResult, error) {
	nilaiWhere := `n.deleted_at IS NULL`
	if kelasID != nil {
		nilaiWhere += fmt.Sprintf(` AND u.mst_kelas_id = %d`, *kelasID)
	}
	if mapelID != nil {
		nilaiWhere += fmt.Sprintf(` AND u.mst_mapel_id = %d`, *mapelID)
	}
	if semester != "" {
		nilaiWhere += fmt.Sprintf(` AND u.semester = '%s'`, semester)
	}

	var (
		totalUjian   int64
		totalNilai   int64
		avgNilai     float64
		passCount    int64
	)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		var err error
		totalUjian, err = countQuery(gctx, r.db, `SELECT COUNT(*) FROM trx_ujian WHERE deleted_at IS NULL`)
		return err
	})
	g.Go(func() error {
		return r.db.QueryRowContext(gctx, fmt.Sprintf(`
			SELECT COUNT(*), COALESCE(AVG(n.nilai), 0)
			FROM trx_nilai n
			JOIN trx_ujian u ON u.id = n.trx_ujian_id
			WHERE %s
		`, nilaiWhere)).Scan(&totalNilai, &avgNilai)
	})
	g.Go(func() error {
		var err error
		passCount, err = countQuery(gctx, r.db, fmt.Sprintf(`
			SELECT COUNT(*) FROM trx_nilai n
			JOIN trx_ujian u ON u.id = n.trx_ujian_id
			WHERE %s AND n.nilai >= %g
		`, nilaiWhere, kkm))
		return err
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	var passRate float64
	if totalNilai > 0 {
		passRate = round2(float64(passCount) / float64(totalNilai) * 100)
	}

	// Pass rate per mapel
	type mapelPassRow struct {
		NamaMapel  string  `db:"nama_mapel"`
		Total      int64   `db:"total"`
		Lulus      int64   `db:"lulus"`
		TidakLulus int64   `db:"tidak_lulus"`
		PassRate   float64 `db:"pass_rate"`
	}
	mapelQ := fmt.Sprintf(`
		SELECT m.nama_mapel,
			COUNT(*) as total,
			SUM(CASE WHEN n.nilai >= %g THEN 1 ELSE 0 END) as lulus,
			SUM(CASE WHEN n.nilai < %g THEN 1 ELSE 0 END) as tidak_lulus,
			ROUND(SUM(CASE WHEN n.nilai >= %g THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) as pass_rate
		FROM trx_nilai n
		JOIN trx_ujian u ON u.id = n.trx_ujian_id
		JOIN mst_mapel m ON m.id = u.mst_mapel_id
		WHERE %s
		GROUP BY m.id, m.nama_mapel
		ORDER BY pass_rate DESC
	`, kkm, kkm, kkm, nilaiWhere)

	var mapelRows []mapelPassRow
	_ = r.db.SelectContext(ctx, &mapelRows, mapelQ)

	prChart := model.PassRateChart{}
	for _, row := range mapelRows {
		prChart.Labels = append(prChart.Labels, row.NamaMapel)
		prChart.Lulus = append(prChart.Lulus, row.Lulus)
		prChart.TidakLulus = append(prChart.TidakLulus, row.TidakLulus)
		prChart.PassRate = append(prChart.PassRate, row.PassRate)
	}

	// Histogram
	histQ := fmt.Sprintf(`
		SELECT
			CASE
				WHEN n.nilai < 60 THEN 'Di bawah 60'
				WHEN n.nilai BETWEEN 60 AND 69 THEN '60 – 69'
				WHEN n.nilai BETWEEN 70 AND 79 THEN '70 – 79'
				WHEN n.nilai BETWEEN 80 AND 89 THEN '80 – 89'
				ELSE '90 – 100'
			END as rentang,
			COUNT(*) as total
		FROM trx_nilai n
		JOIN trx_ujian u ON u.id = n.trx_ujian_id
		WHERE %s
		GROUP BY rentang
	`, nilaiWhere)

	type histRow struct {
		Rentang string `db:"rentang"`
		Total   int64  `db:"total"`
	}
	var histRows []histRow
	_ = r.db.SelectContext(ctx, &histRows, histQ)

	histLabels := []string{"Di bawah 60", "60 – 69", "70 – 79", "80 – 89", "90 – 100"}
	histColors := []string{"#EF4444", "#F59E0B", "#FBBF24", "#34D399", "#10B981"}
	histMap := map[string]int64{}
	for _, h := range histRows {
		histMap[h.Rentang] = h.Total
	}
	histData := make([]int64, len(histLabels))
	for i, l := range histLabels {
		histData[i] = histMap[l]
	}

	// Top performers
	topQ := fmt.Sprintf(`
		SELECT s.id, s.nama, s.nis, k.nama_kelas, ROUND(AVG(n.nilai)::numeric, 2) as rata_rata
		FROM trx_nilai n
		JOIN trx_ujian u ON u.id = n.trx_ujian_id
		JOIN mst_siswa s ON s.id = n.mst_siswa_id
		JOIN mst_kelas k ON k.id = s.mst_kelas_id
		WHERE %s
		GROUP BY s.id, s.nama, s.nis, k.nama_kelas
		ORDER BY rata_rata DESC
		LIMIT 10
	`, nilaiWhere)

	var topSiswa []model.TopSiswaItem
	_ = r.db.SelectContext(ctx, &topSiswa, topQ)

	return &model.UjianResult{
		Summary: model.UjianSummary{
			TotalUjian:     totalUjian,
			TotalNilai:     totalNilai,
			AvgNilaiGlobal: round2(avgNilai),
			PassRate:       passRate,
			KKM:            kkm,
		},
		PassRateChart: prChart,
		Histogram: model.NilaiHistogram{
			Labels: histLabels,
			Data:   histData,
			Colors: histColors,
		},
		TopPerformer: topSiswa,
		Tahun:        time.Now().Year(),
	}, nil
}

// ─── EKSTRAKURIKULER ───────────────────────────────────────────────────────

func (r *StatistikRepo) GetEkstrakurikuler(ctx context.Context, tahun int) (*model.EkstrakurikulerResult, error) {
	var totalEkskul, totalPeserta, totalSiswa int64

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		var err error
		totalEkskul, err = countQuery(gctx, r.db, `SELECT COUNT(*) FROM mst_ekstrakurikuler WHERE deleted_at IS NULL`)
		return err
	})
	g.Go(func() error {
		var err error
		totalPeserta, err = countQuery(gctx, r.db, `SELECT COUNT(DISTINCT siswa_id) FROM trx_ekstrakurikuler_siswa`)
		return err
	})
	g.Go(func() error {
		var err error
		totalSiswa, err = countQuery(gctx, r.db, `SELECT COUNT(*) FROM mst_siswa WHERE deleted_at IS NULL`)
		return err
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	var partisipasiRate float64
	if totalSiswa > 0 {
		partisipasiRate = round2(float64(totalPeserta) / float64(totalSiswa) * 100)
	}

	// Distribusi per ekskul
	type ekskulRow struct {
		NamaEkstrakurikuler string `db:"nama_ekstrakurikuler"`
		TotalPeserta        int64  `db:"total_peserta"`
	}
	var perEkskul []ekskulRow
	_ = r.db.SelectContext(ctx, &perEkskul, `
		SELECT e.nama as nama_ekstrakurikuler, COUNT(*) as total_peserta
		FROM trx_ekstrakurikuler_siswa tes
		JOIN mst_ekstrakurikuler e ON e.id = tes.ekstrakurikuler_id
		GROUP BY e.id, e.nama
		ORDER BY total_peserta DESC
	`)

	distribusiEkskul := model.ChartSeries{Colors: generateColors(len(perEkskul))}
	for _, e := range perEkskul {
		distribusiEkskul.Labels = append(distribusiEkskul.Labels, e.NamaEkstrakurikuler)
		distribusiEkskul.Data = append(distribusiEkskul.Data, e.TotalPeserta)
	}

	// Tren pendaftaran per bulan
	type monthRow struct {
		Bulan int   `db:"bulan"`
		Total int64 `db:"total"`
	}
	var trenRows []monthRow
	_ = r.db.SelectContext(ctx, &trenRows, fmt.Sprintf(`
		SELECT EXTRACT(MONTH FROM created_at)::int as bulan, COUNT(*) as total
		FROM trx_ekstrakurikuler_siswa
		WHERE EXTRACT(YEAR FROM created_at) = %d
		GROUP BY EXTRACT(MONTH FROM created_at)::int
		ORDER BY bulan
	`, tahun))

	trenMap := map[int]int64{}
	for _, t := range trenRows {
		trenMap[t.Bulan] = t.Total
	}
	trenLabels := make([]string, 12)
	trenData := make([]int64, 12)
	for m := 1; m <= 12; m++ {
		trenLabels[m-1] = namaBulan(m)
		trenData[m-1] = trenMap[m]
	}

	// Per kelas
	type kelasRow struct {
		NamaKelas string `db:"nama_kelas"`
		Total     int64  `db:"total"`
	}
	var perKelas []kelasRow
	_ = r.db.SelectContext(ctx, &perKelas, `
		SELECT k.nama_kelas, COUNT(DISTINCT tes.siswa_id) as total
		FROM trx_ekstrakurikuler_siswa tes
		JOIN mst_siswa s ON s.id = tes.siswa_id
		JOIN mst_kelas k ON k.id = s.mst_kelas_id
		GROUP BY k.id, k.nama_kelas
		ORDER BY total DESC
	`)

	distribusiKelas := model.ChartSeries{Colors: generateColors(len(perKelas))}
	for _, k := range perKelas {
		distribusiKelas.Labels = append(distribusiKelas.Labels, k.NamaKelas)
		distribusiKelas.Data = append(distribusiKelas.Data, k.Total)
	}

	return &model.EkstrakurikulerResult{
		Summary: model.EkstrakurikulerSummary{
			TotalEkstrakurikuler: totalEkskul,
			TotalPeserta:         totalPeserta,
			PartisipasiRate:      partisipasiRate,
		},
		DistribusiPerEkskul: distribusiEkskul,
		TrenPendaftaran: model.ChartSeries{
			Labels: trenLabels,
			Data:   trenData,
			Color:  "#8B5CF6",
		},
		DistribusiPerKelas: distribusiKelas,
		Tahun:              tahun,
	}, nil
}

// ─── ORGANISASI ────────────────────────────────────────────────────────────

func (r *StatistikRepo) GetOrganisasi(ctx context.Context) (*model.OrganisasiResult, error) {
	var totalOrg, totalAnggota, totalSiswa int64

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		var err error
		totalOrg, err = countQuery(gctx, r.db, `SELECT COUNT(*) FROM mst_organisasi WHERE deleted_at IS NULL`)
		return err
	})
	g.Go(func() error {
		var err error
		totalAnggota, err = countQuery(gctx, r.db, `SELECT COUNT(DISTINCT siswa_id) FROM trx_organisasi_anggota`)
		return err
	})
	g.Go(func() error {
		var err error
		totalSiswa, err = countQuery(gctx, r.db, `SELECT COUNT(*) FROM mst_siswa WHERE deleted_at IS NULL`)
		return err
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	var partisipasiRate float64
	if totalSiswa > 0 {
		partisipasiRate = round2(float64(totalAnggota) / float64(totalSiswa) * 100)
	}

	type labelTotal struct {
		Nama  string `db:"nama"`
		Total int64  `db:"total"`
	}

	var perOrg []labelTotal
	_ = r.db.SelectContext(ctx, &perOrg, `
		SELECT o.nama, COUNT(*) as total
		FROM trx_organisasi_anggota a
		JOIN mst_organisasi o ON o.id = a.organisasi_id
		GROUP BY o.id, o.nama
		ORDER BY total DESC
	`)

	distribusiOrg := model.ChartSeries{Colors: generateColors(len(perOrg))}
	for _, o := range perOrg {
		distribusiOrg.Labels = append(distribusiOrg.Labels, o.Nama)
		distribusiOrg.Data = append(distribusiOrg.Data, o.Total)
	}

	var perJabatan []labelTotal
	_ = r.db.SelectContext(ctx, &perJabatan, `
		SELECT j.nama, COUNT(*) as total
		FROM trx_organisasi_anggota a
		JOIN mst_organisasi_jabatan j ON j.id = a.jabatan_id
		GROUP BY j.id, j.nama
		ORDER BY total DESC
	`)

	distribusiJabatan := model.ChartSeries{Colors: generateColors(len(perJabatan))}
	for _, j := range perJabatan {
		distribusiJabatan.Labels = append(distribusiJabatan.Labels, j.Nama)
		distribusiJabatan.Data = append(distribusiJabatan.Data, j.Total)
	}

	var perKelas []labelTotal
	_ = r.db.SelectContext(ctx, &perKelas, `
		SELECT k.nama_kelas as nama, COUNT(DISTINCT a.siswa_id) as total
		FROM trx_organisasi_anggota a
		JOIN mst_siswa s ON s.id = a.siswa_id
		JOIN mst_kelas k ON k.id = s.mst_kelas_id
		GROUP BY k.id, k.nama_kelas
		ORDER BY total DESC
	`)

	distribusiKelas := model.ChartSeries{Colors: generateColors(len(perKelas))}
	for _, k := range perKelas {
		distribusiKelas.Labels = append(distribusiKelas.Labels, k.Nama)
		distribusiKelas.Data = append(distribusiKelas.Data, k.Total)
	}

	return &model.OrganisasiResult{
		Summary: model.OrganisasiSummary{
			TotalOrganisasi: totalOrg,
			TotalAnggota:    totalAnggota,
			PartisipasiRate: partisipasiRate,
		},
		DistribusiPerOrganisasi: distribusiOrg,
		DistribusiPerJabatan:    distribusiJabatan,
		DistribusiPerKelas:      distribusiKelas,
	}, nil
}

// ─── GURU ─────────────────────────────────────────────────────────────────

func (r *StatistikRepo) GetGuru(ctx context.Context, startDate, endDate string) (*model.GuruResult, error) {
	var totalGuru, guruHadir, guruTotal int64

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		var err error
		totalGuru, err = countQuery(gctx, r.db, `SELECT COUNT(*) FROM mst_guru WHERE deleted_at IS NULL`)
		return err
	})
	g.Go(func() error {
		var err error
		guruHadir, err = countQuery(gctx, r.db,
			`SELECT COUNT(*) FROM trx_absensi_guru WHERE tanggal BETWEEN $1 AND $2 AND jam_masuk IS NOT NULL AND deleted_at IS NULL`,
			startDate, endDate)
		return err
	})
	g.Go(func() error {
		var err error
		guruTotal, err = countQuery(gctx, r.db,
			`SELECT COUNT(*) FROM trx_absensi_guru WHERE tanggal BETWEEN $1 AND $2 AND deleted_at IS NULL`,
			startDate, endDate)
		return err
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	var hadirRate float64
	if guruTotal > 0 {
		hadirRate = round2(float64(guruHadir) / float64(guruTotal) * 100)
	}

	// Distribusi guru per mapel
	type mapelRow struct {
		NamaMapel string `db:"nama_mapel"`
		Total     int64  `db:"total_guru"`
	}
	var perMapel []mapelRow
	_ = r.db.SelectContext(ctx, &perMapel, `
		SELECT m.nama_mapel, COUNT(*) as total_guru
		FROM mst_guru g
		JOIN mst_guru_mapel gm ON gm.mst_guru_id = g.id
		JOIN mst_mapel m ON m.id = gm.mst_mapel_id
		WHERE g.deleted_at IS NULL
		GROUP BY m.id, m.nama_mapel
		ORDER BY total_guru DESC
	`)

	distribusiMapel := model.ChartSeries{Colors: generateColors(len(perMapel))}
	for _, m := range perMapel {
		distribusiMapel.Labels = append(distribusiMapel.Labels, m.NamaMapel)
		distribusiMapel.Data = append(distribusiMapel.Data, m.Total)
	}

	// Breakdown kehadiran guru
	type statusRow struct {
		HadirCount int64 `db:"hadir"`
		IzinCount  int64 `db:"izin"`
		SakitCount int64 `db:"sakit"`
		AlphaCount int64 `db:"alpha"`
	}
	var breakdown statusRow
	_ = r.db.QueryRowContext(ctx, `
		SELECT
			SUM(CASE WHEN jam_masuk IS NOT NULL THEN 1 ELSE 0 END) as hadir,
			0 as izin, 0 as sakit,
			SUM(CASE WHEN jam_masuk IS NULL THEN 1 ELSE 0 END) as alpha
		FROM trx_absensi_guru
		WHERE tanggal BETWEEN $1 AND $2 AND deleted_at IS NULL
	`, startDate, endDate).Scan(&breakdown.HadirCount, &breakdown.IzinCount, &breakdown.SakitCount, &breakdown.AlphaCount)

	// Tren kehadiran guru per hari
	var trenRows []model.GuruTrenItem
	_ = r.db.SelectContext(ctx, &trenRows, `
		SELECT tanggal::text, COUNT(*) as total,
			SUM(CASE WHEN jam_masuk IS NOT NULL THEN 1 ELSE 0 END) as hadir
		FROM trx_absensi_guru
		WHERE tanggal BETWEEN $1 AND $2 AND deleted_at IS NULL
		GROUP BY tanggal
		ORDER BY tanggal
	`, startDate, endDate)

	return &model.GuruResult{
		Summary: model.GuruSummary{
			TotalGuru:        totalGuru,
			HadirRate:        hadirRate,
			TotalHadir:       guruHadir,
			TotalAbsensiHari: guruTotal,
		},
		DistribusiPerMapel: distribusiMapel,
		KehadiranBreakdown: model.GuruBreakdown{
			Hadir: breakdown.HadirCount,
			Izin:  breakdown.IzinCount,
			Sakit: breakdown.SakitCount,
			Alpha: breakdown.AlphaCount,
		},
		TrenKehadiranGuru: trenRows,
		Periode: map[string]string{
			"start": startDate,
			"end":   endDate,
		},
	}, nil
}

// ─── SPK ──────────────────────────────────────────────────────────────────

func (r *StatistikRepo) GetSPK(ctx context.Context) (*model.SPKResult, error) {
	var totalEvaluasi int64
	var avgSkor, skorTertinggi, skorTerendah float64

	_ = r.db.QueryRowContext(ctx, `
		SELECT COUNT(*), COALESCE(AVG(skor_akhir), 0),
			COALESCE(MAX(skor_akhir), 0), COALESCE(MIN(skor_akhir), 0)
		FROM spk_hasil WHERE deleted_at IS NULL
	`).Scan(&totalEvaluasi, &avgSkor, &skorTertinggi, &skorTerendah)

	// Distribusi skor (range bands)
	type distribRow struct {
		Rentang string `db:"rentang"`
		Total   int64  `db:"total"`
	}
	var distribRows []distribRow
	_ = r.db.SelectContext(ctx, &distribRows, `
		SELECT
			CASE
				WHEN skor_akhir < 0.2 THEN '0.0 – 0.2'
				WHEN skor_akhir < 0.4 THEN '0.2 – 0.4'
				WHEN skor_akhir < 0.6 THEN '0.4 – 0.6'
				WHEN skor_akhir < 0.8 THEN '0.6 – 0.8'
				ELSE '0.8 – 1.0'
			END as rentang,
			COUNT(*) as total
		FROM spk_hasil WHERE deleted_at IS NULL
		GROUP BY rentang
		ORDER BY rentang
	`)

	rentangLabels := []string{"0.0 – 0.2", "0.2 – 0.4", "0.4 – 0.6", "0.6 – 0.8", "0.8 – 1.0"}
	rentangMap := map[string]int64{}
	for _, d := range distribRows {
		rentangMap[d.Rentang] = d.Total
	}
	distribData := make([]int64, len(rentangLabels))
	for i, l := range rentangLabels {
		distribData[i] = rentangMap[l]
	}

	// Rata-rata per kriteria
	var kriteriaAvg []model.KriteriaAvg
	_ = r.db.SelectContext(ctx, &kriteriaAvg, `
		SELECT k.nama as nama_kriteria, ROUND(AVG(p.nilai_raw)::numeric, 2) as rata_rata
		FROM spk_penilaian p
		JOIN spk_kriteria k ON k.id = p.spk_kriteria_id
		WHERE p.deleted_at IS NULL
		GROUP BY k.id, k.nama
		ORDER BY rata_rata DESC
	`)

	// Top siswa by skor
	var topSiswa []model.SPKSiswaItem
	_ = r.db.SelectContext(ctx, &topSiswa, `
		SELECT s.id, s.nama, s.nis, k.nama_kelas,
			h.skor_akhir, h.peringkat
		FROM spk_hasil h
		JOIN mst_siswa s ON s.id = h.mst_siswa_id
		JOIN mst_kelas k ON k.id = s.mst_kelas_id
		WHERE h.deleted_at IS NULL
		ORDER BY h.skor_akhir DESC
		LIMIT 10
	`)

	return &model.SPKResult{
		Summary: model.SPKSummary{
			TotalEvaluasi: totalEvaluasi,
			RataRataSkor:  round2(avgSkor),
			SkorTertinggi: round2(skorTertinggi),
			SkorTerendah:  round2(skorTerendah),
		},
		Distribusi: model.SPKDistribusi{
			Labels: rentangLabels,
			Data:   distribData,
			Colors: []string{"#EF4444", "#F59E0B", "#FBBF24", "#34D399", "#10B981"},
		},
		RataRataKriteria: kriteriaAvg,
		TopSiswa:         topSiswa,
	}, nil
}
