package model

// UserClaims holds authenticated user data loaded from DB after JWT validation.
type UserClaims struct {
	UserID  int64
	Roles   []string
	SiswaID *int64
	GuruID  *int64
}

func (c *UserClaims) HasRole(role string) bool {
	for _, r := range c.Roles {
		if r == role {
			return true
		}
	}
	return false
}

// ─── Common chart structures ───────────────────────────────────────────────

type ChartSeries struct {
	Labels []string `json:"labels"`
	Data   []int64  `json:"data"`
	Color  string   `json:"color,omitempty"`
	Colors []string `json:"colors,omitempty"`
}

type MultiSeriesChart struct {
	Labels   []string      `json:"labels"`
	Datasets []DatasetItem `json:"datasets"`
}

type DatasetItem struct {
	Label string  `json:"label"`
	Data  []int64 `json:"data"`
	Color string  `json:"color"`
}

// ─── Overview ─────────────────────────────────────────────────────────────

type OverviewSummary struct {
	TotalSiswa      int64 `json:"total_siswa"`
	TotalGuru       int64 `json:"total_guru"`
	TotalKelas      int64 `json:"total_kelas"`
	HadirHariIni    int64 `json:"hadir_hari_ini"`
	KasusBKProses   int64 `json:"kasus_bk_proses"`
	PendaftarPPDB   int64 `json:"pendaftar_ppdb"`
	BukuDipinjam    int64 `json:"buku_dipinjam"`
	HasilSPK        int64 `json:"hasil_spk"`
}

type OverviewResult struct {
	Summary        OverviewSummary      `json:"summary"`
	KPICards       []KPICard            `json:"kpi_cards"`
	Sparkline7Days SparklineData        `json:"sparkline_7_days"`
	Meta           map[string]string    `json:"meta"`
}

type KPICard struct {
	ID         string `json:"id"`
	Title      string `json:"title"`
	Value      string `json:"value"`
	Icon       string `json:"icon"`
	Color      string `json:"color"`
	BgColor    string `json:"bg_color"`
	Trend      string `json:"trend"`
	TrendLabel string `json:"trend_label"`
	SubValue   string `json:"sub_value"`
}

type SparklineData struct {
	Labels        []string `json:"labels"`
	KehadiranSiswa []int64 `json:"kehadiran_siswa"`
	PendapatanSPP  []int64 `json:"pendapatan_spp"`
	KasusBK        []int64 `json:"kasus_bk"`
}

// ─── Akademik ─────────────────────────────────────────────────────────────

type AkademikResult struct {
	Summary          AkademikSummary  `json:"summary"`
	DistribusiGender ChartSeries      `json:"distribusi_gender"`
	DistribusiKelas  ChartSeries      `json:"distribusi_kelas"`
	NilaiHistogram   NilaiHistogram   `json:"nilai_histogram"`
	TopSiswa         []TopSiswaItem   `json:"top_siswa"`
	NilaiPerMapel    ChartSeries      `json:"nilai_per_mapel"`
}

type AkademikSummary struct {
	TotalSiswa      int64   `json:"total_siswa"`
	TotalGuru       int64   `json:"total_guru"`
	TotalKelas      int64   `json:"total_kelas"`
	RataRataNilai   float64 `json:"rata_rata_nilai"`
}

type NilaiHistogram struct {
	Labels []string `json:"labels"`
	Data   []int64  `json:"data"`
	Colors []string `json:"colors"`
}

type TopSiswaItem struct {
	ID        int64   `json:"id" db:"id"`
	Nama      string  `json:"nama" db:"nama"`
	NIS       string  `json:"nis" db:"nis"`
	NamaKelas string  `json:"nama_kelas" db:"nama_kelas"`
	RataRata  float64 `json:"rata_rata" db:"rata_rata"`
}

// ─── Kehadiran ─────────────────────────────────────────────────────────────

type KehadiranResult struct {
	Summary        KehadiranSummary `json:"summary"`
	TrenBulanan    MultiSeriesChart `json:"tren_bulanan"`
	StatusDonut    ChartSeries      `json:"status_donut"`
	PerKelas       ChartSeries      `json:"per_kelas"`
	GuruKehadiran  GuruKehadiran    `json:"guru_kehadiran"`
}

type KehadiranSummary struct {
	TotalHadir    int64   `json:"total_hadir"`
	TotalIzin     int64   `json:"total_izin"`
	TotalSakit    int64   `json:"total_sakit"`
	TotalAlpha    int64   `json:"total_alpha"`
	PersentaseHadir float64 `json:"persentase_hadir"`
}

type GuruKehadiran struct {
	TotalHadir     int64   `json:"total_hadir"`
	TotalAbsensi   int64   `json:"total_absensi"`
	PersentaseHadir float64 `json:"persentase_hadir"`
}

// ─── Keuangan ─────────────────────────────────────────────────────────────

type KeuanganResult struct {
	Summary        KeuanganSummary  `json:"summary"`
	TrenBulanan    ChartSeries      `json:"tren_bulanan"`
	StatusDistribusi ChartSeries    `json:"status_distribusi"`
	TunggakanPerKelas []TunggakanKelas `json:"tunggakan_per_kelas"`
}

type KeuanganSummary struct {
	TotalPendapatan  int64   `json:"total_pendapatan"`
	TotalLunas       int64   `json:"total_lunas"`
	TotalBelumLunas  int64   `json:"total_belum_lunas"`
	PersentaseLunas  float64 `json:"persentase_lunas"`
}

type TunggakanKelas struct {
	NamaKelas    string `json:"nama_kelas" db:"nama_kelas"`
	TotalTunggak int64  `json:"total_tunggak" db:"total_tunggak"`
}

// ─── BK ───────────────────────────────────────────────────────────────────

type BKResult struct {
	Summary            BKSummary      `json:"summary"`
	TrenKasusBulanan   ChartSeries    `json:"tren_kasus_bulanan"`
	StatusDistribution ChartSeries    `json:"status_distribution"`
	DistribusiKategori ChartSeries    `json:"distribusi_kategori"`
	DistribusiJenis    ChartSeries    `json:"distribusi_jenis"`
	DistribusiPerKelas ChartSeries    `json:"distribusi_per_kelas"`
	SiswaCasingTerbanyak []SiswaBKItem `json:"siswa_kasus_terbanyak"`
	Tahun              int            `json:"tahun"`
}

type BKSummary struct {
	TotalKasus       int64   `json:"total_kasus"`
	KasusProses      int64   `json:"kasus_proses"`
	KasusSelesai     int64   `json:"kasus_selesai"`
	ResolusiRate     float64 `json:"resolusi_rate"`
	AvgResolusiHari  float64 `json:"avg_resolusi_hari"`
}

type SiswaBKItem struct {
	ID         int64  `json:"id" db:"id"`
	Nama       string `json:"nama" db:"nama"`
	NIS        string `json:"nis" db:"nis"`
	NamaKelas  string `json:"nama_kelas" db:"nama_kelas"`
	TotalKasus int64  `json:"total_kasus" db:"total_kasus"`
}

// ─── PPDB ─────────────────────────────────────────────────────────────────

type PPDBResult struct {
	Summary             PPDBSummary         `json:"summary"`
	Funnel              []FunnelItem         `json:"funnel"`
	TrenPendaftaran     ChartSeries          `json:"tren_pendaftaran"`
	DistribusiGelombang DistribusiGelombang  `json:"distribusi_gelombang"`
	Tahun               int                  `json:"tahun"`
}

type PPDBSummary struct {
	TotalPendaftar   int64   `json:"total_pendaftar"`
	TotalDiterima    int64   `json:"total_diterima"`
	TotalDitolak     int64   `json:"total_ditolak"`
	AcceptanceRate   float64 `json:"acceptance_rate"`
	YoYGrowthPct     float64 `json:"yoy_growth_pct"`
	PrevYearTotal    int64   `json:"prev_year_total"`
	PrevYearDiterima int64   `json:"prev_year_diterima"`
}

type FunnelItem struct {
	Stage string `json:"stage"`
	Total int64  `json:"total"`
	Color string `json:"color"`
}

type DistribusiGelombang struct {
	Labels         []string  `json:"labels"`
	TotalPendaftar []int64   `json:"total_pendaftar"`
	TotalDiterima  []int64   `json:"total_diterima"`
	AcceptanceRate []float64 `json:"acceptance_rate"`
	Colors         []string  `json:"colors"`
}

// ─── Perpustakaan ──────────────────────────────────────────────────────────

type PerpustakaanResult struct {
	Summary          PerpustakaanSummary `json:"summary"`
	TrenPeminjaman   MultiSeriesChart    `json:"tren_peminjaman"`
	TopBukuDiminati  []BukuItem          `json:"top_buku_diminati"`
	SiswaAktifPinjam []SiswaPinjamItem   `json:"siswa_aktif_pinjam"`
	Tahun            int                 `json:"tahun"`
}

type PerpustakaanSummary struct {
	TotalJudulBuku  int64   `json:"total_judul_buku"`
	SedangDipinjam  int64   `json:"sedang_dipinjam"`
	Overdue         int64   `json:"overdue"`
	UtilizationRate float64 `json:"utilization_rate"`
}

type BukuItem struct {
	Judul        string `json:"judul" db:"judul"`
	Penulis      string `json:"penulis" db:"penulis"`
	TotalDipinjam int64 `json:"total_dipinjam" db:"total_dipinjam"`
}

type SiswaPinjamItem struct {
	Nama       string `json:"nama" db:"nama"`
	NIS        string `json:"nis" db:"nis"`
	NamaKelas  string `json:"nama_kelas" db:"nama_kelas"`
	TotalPinjam int64 `json:"total_pinjam" db:"total_pinjam"`
}

// ─── Ujian ────────────────────────────────────────────────────────────────

type UjianResult struct {
	Summary       UjianSummary    `json:"summary"`
	PassRateChart PassRateChart   `json:"pass_rate_chart"`
	Histogram     NilaiHistogram  `json:"histogram"`
	TopPerformer  []TopSiswaItem  `json:"top_performers"`
	Tahun         int             `json:"tahun"`
}

type UjianSummary struct {
	TotalUjian      int64   `json:"total_ujian"`
	TotalNilai      int64   `json:"total_nilai"`
	AvgNilaiGlobal  float64 `json:"avg_nilai_global"`
	PassRate        float64 `json:"pass_rate"`
	KKM             float64 `json:"kkm"`
}

type PassRateChart struct {
	Labels    []string  `json:"labels"`
	Lulus     []int64   `json:"lulus"`
	TidakLulus []int64  `json:"tidak_lulus"`
	PassRate  []float64 `json:"pass_rate"`
}

// ─── Ekstrakurikuler ───────────────────────────────────────────────────────

type EkstrakurikulerResult struct {
	Summary              EkstrakurikulerSummary `json:"summary"`
	DistribusiPerEkskul  ChartSeries            `json:"distribusi_per_ekskul"`
	TrenPendaftaran      ChartSeries            `json:"tren_pendaftaran"`
	DistribusiPerKelas   ChartSeries            `json:"distribusi_per_kelas"`
	Tahun                int                    `json:"tahun"`
}

type EkstrakurikulerSummary struct {
	TotalEkstrakurikuler int64   `json:"total_ekstrakurikuler"`
	TotalPeserta         int64   `json:"total_peserta"`
	PartisipasiRate      float64 `json:"partisipasi_rate"`
}

// ─── Organisasi ────────────────────────────────────────────────────────────

type OrganisasiResult struct {
	Summary                OrganisasiSummary `json:"summary"`
	DistribusiPerOrganisasi ChartSeries      `json:"distribusi_per_organisasi"`
	DistribusiPerJabatan   ChartSeries       `json:"distribusi_per_jabatan"`
	DistribusiPerKelas     ChartSeries       `json:"distribusi_per_kelas"`
}

type OrganisasiSummary struct {
	TotalOrganisasi int64   `json:"total_organisasi"`
	TotalAnggota    int64   `json:"total_anggota"`
	PartisipasiRate float64 `json:"partisipasi_rate"`
}

// ─── Guru ─────────────────────────────────────────────────────────────────

type GuruResult struct {
	Summary           GuruSummary      `json:"summary"`
	DistribusiPerMapel ChartSeries     `json:"distribusi_per_mapel"`
	KehadiranBreakdown GuruBreakdown   `json:"kehadiran_breakdown"`
	TrenKehadiranGuru  []GuruTrenItem  `json:"tren_kehadiran_guru"`
	Periode            map[string]string `json:"periode"`
}

type GuruSummary struct {
	TotalGuru      int64   `json:"total_guru"`
	HadirRate      float64 `json:"hadir_rate"`
	TotalHadir     int64   `json:"total_hadir"`
	TotalAbsensiHari int64 `json:"total_absensi_hari"`
}

type GuruBreakdown struct {
	Hadir int64 `json:"hadir"`
	Izin  int64 `json:"izin"`
	Sakit int64 `json:"sakit"`
	Alpha int64 `json:"alpha"`
}

type GuruTrenItem struct {
	Tanggal string `json:"tanggal" db:"tanggal"`
	Total   int64  `json:"total" db:"total"`
	Hadir   int64  `json:"hadir" db:"hadir"`
}

// ─── SPK ──────────────────────────────────────────────────────────────────

type SPKResult struct {
	Summary        SPKSummary    `json:"summary"`
	Distribusi     SPKDistribusi `json:"distribusi"`
	RataRataKriteria []KriteriaAvg `json:"rata_rata_kriteria"`
	TopSiswa       []SPKSiswaItem `json:"top_siswa"`
}

type SPKSummary struct {
	TotalEvaluasi int64   `json:"total_evaluasi"`
	RataRataSkor  float64 `json:"rata_rata_skor"`
	SkorTertinggi float64 `json:"skor_tertinggi"`
	SkorTerendah  float64 `json:"skor_terendah"`
}

type SPKDistribusi struct {
	Labels []string `json:"labels"`
	Data   []int64  `json:"data"`
	Colors []string `json:"colors"`
}

type KriteriaAvg struct {
	NamaKriteria string  `json:"nama_kriteria" db:"nama_kriteria"`
	RataRata     float64 `json:"rata_rata" db:"rata_rata"`
}

type SPKSiswaItem struct {
	ID        int64   `json:"id" db:"id"`
	Nama      string  `json:"nama" db:"nama"`
	NIS       string  `json:"nis" db:"nis"`
	NamaKelas string  `json:"nama_kelas" db:"nama_kelas"`
	SkorAkhir float64 `json:"skor_akhir" db:"skor_akhir"`
	Peringkat int     `json:"peringkat" db:"peringkat"`
}
