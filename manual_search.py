import asyncio
import time
import random
import string
import csv
import re
import os
import urllib.request
import urllib.parse
import json
from dotenv import load_dotenv
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from google import genai
from google.genai import types
# Tambahkan import ini di bagian atas main.py
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Memuat variabel dari file .env
load_dotenv()
 
WEB_APP_SCRIPT_URL = os.getenv("WEB_APP_SCRIPT_URL") 
# Muat ketiga API Key
GEMINI_API_KEY_1 = os.getenv("GEMINI_API_KEY_1")
GEMINI_API_KEY_2 = os.getenv("GEMINI_API_KEY_2")
GEMINI_API_KEY_3 = os.getenv("GEMINI_API_KEY_3")
GEMINI_API_KEY_4 = os.getenv("GEMINI_API_KEY_4")

# =====================================================================
# INISIALISASI GOOGLE SHEETS API
# =====================================================================
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

def inisialisasi_sheets_client_from_env():
    client_email = os.getenv("GOOGLE_CLIENT_EMAIL")
    private_key = os.getenv("GOOGLE_PRIVATE_KEY")
    
    if not client_email or not private_key:
        print("[!] Kredensial GOOGLE_CLIENT_EMAIL atau GOOGLE_PRIVATE_KEY tidak ditemukan di .env")
        return None
        
    formatted_private_key = private_key.replace('\\n', '\n')
    
    info = {
        "type": "service_account",
        "client_email": client_email,
        "private_key": formatted_private_key,
        "token_uri": "https://oauth2.googleapis.com/token"
    }
    
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    return service.spreadsheets()

try:
    sheets_client = inisialisasi_sheets_client_from_env()
except Exception as e:
    print(f"[!] Gagal inisialisasi Google Sheets API: {e}")
    sheets_client = None


# Masukkan ke dalam list dan abaikan yang kosong (jika sewaktu-waktu Anda hanya pakai 2 key)
api_keys = [k for k in [GEMINI_API_KEY_1, GEMINI_API_KEY_2, GEMINI_API_KEY_3, GEMINI_API_KEY_4] if k]

if not api_keys:
    raise ValueError("Tidak ada GEMINI_API_KEY yang ditemukan di dalam file .env!")

# Inisialisasi multiple client untuk setiap API Key
clients = [genai.Client(api_key=key) for key in api_keys]

# Class untuk merotasi pemakaian client (Round-Robin) secara aman (thread-safe)
class ClientRotator:
    def __init__(self, api_keys):
        self.api_keys = api_keys
        # Ubah menjadi pasangan tuple: (objek_client, string_api_key_asli)
        self.clients = [(genai.Client(api_key=key), key) for key in api_keys]
        self.index = 0

    async def get_client(self):
        if not self.clients:
            return None, None
        client_obj, api_key = self.clients[self.index]
        self.index = (self.index + 1) % len(self.clients)
        return client_obj, api_key # Kembalikan berpasangan

client_rotator = ClientRotator(api_keys)
gemini_concurrency_limiter = asyncio.Semaphore(len(api_keys))

# =====================================================================
# SYSTEM SMART RATE LIMITER
# =====================================================================
class SmartRateLimiter:
    def __init__(self, max_requests=12, window_seconds=70):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.request_history = [] 
        self.lock = asyncio.Lock()

    async def acquire(self):
        while True:
            async with self.lock:
                now = time.time()
                # Bersihkan riwayat lama
                self.request_history = [t for t in self.request_history if now - t < self.window_seconds]
                
                if len(self.request_history) < self.max_requests:
                    self.request_history.append(now)
                    return
                
                sleep_time = self.window_seconds - (now - self.request_history[0])
            
            # Tidur dilakukan di LUAR scope 'async with self.lock' agar tidak mengunci task lain
            if sleep_time > 0:
                print(f"    [!] Kuota Gemini Penuh ({self.max_requests} req / {self.window_seconds}s). Menunggu {sleep_time:.2f} detik...")
                await asyncio.sleep(sleep_time)
            else:
                # Jika hitungan terlalu mepet/negatif, beri jeda paksa 1 detik sebelum cek ulang
                await asyncio.sleep(2)

# Inisialisasi limiter global (Otomatis menyesuaikan jumlah API Key: misal 3 key x 12 = 36 req/70s)
kapasitas_maksimal = 12 * len(api_keys)
gemini_limiter = SmartRateLimiter(max_requests=kapasitas_maksimal, window_seconds=70)
print(f"[*] Sistem Rate Limiter diatur ke {gemini_limiter.max_requests} request per {gemini_limiter.window_seconds} detik.")

# ==========================================
# AMBIL DATA DINAMIS DARI GOOGLE APPS SCRIPT
# ==========================================
async def fetch_dynamic_config(url, max_retries=3, retry_delay=5):
    print("[-] Mengambil konfigurasi dinamis (Websites, Prompts, Tickers) dari Google Spreadsheet...")
    
    # Pindahkan definisi ke sini agar hanya dibuat satu kali di memori
    def fetch_url_sync(target_url):
        with urllib.request.urlopen(target_url, timeout=15) as response:
            return json.loads(response.read().decode("utf-8"))

    for attempt in range(1, max_retries + 1):
        try:
            # Panggil menggunakan to_thread
            res_data = await asyncio.to_thread(fetch_url_sync, url)
            return res_data.get("pencarian_manual", {})
                
        except Exception as e:
            print(f"    [!] Percobaan ke-{attempt} gagal: {e}")
            if attempt < max_retries:
                print(f"    [-] Menunggu {retry_delay} detik sebelum mencoba kembali...")
                await asyncio.sleep(retry_delay)
            else:
                # Alarm terakhir ke Telegram jika benar-benar gagal setelah 3 kali percobaan
                error_msg = (
                    f"🚨 <b>CRITICAL ERROR</b>\n\n"
                    f"Gagal mengambil konfigurasi dari Spreadsheet setelah {max_retries} kali percobaan.\n"
                    f"<b>Detail Kendala:</b> <code>{e}</code>\n\n"
                    f"Sistem otomatis dihentikan."
                )
                # Catatan: Pastikan fungsi send_telegram_message Anda memang fungsi sinkron (bukan async def). 
                # Jika sudah async def, cukup gunakan: await send_telegram_message(error_msg)
                print(error_msg)
                
    return {}

def get_existing_data(spreadsheet_id, target_sheet_name):
    """Mengambil Nama Usaha, Kota, serta Latitude dan Longitude dari Sheets untuk deteksi duplikat."""
    try:
        # Mengambil kolom D (Nama Usaha) sampai I (Longitude)
        # D=4, E=5, F=6, G=7, H=8 (Lat), I=9 (Lon)
        range_to_read = f"{target_sheet_name}!D:I"
        result = sheets_client.values().get(spreadsheetId=spreadsheet_id, range=range_to_read).execute()
        rows = result.get('values', [])
        
        existing_keys = set()
        existing_coords = set()
        
        for row in rows:
            # Cek duplikat Nama Usaha + Kota (Kolom D dan F)
            if len(row) >= 3:
                key = f"{row[0].strip().lower()}|{row[2].strip().lower()}"
                existing_keys.add(key)
            
            # Cek data koordinat (Kolom H dan I -> indeks 4 dan 5 di slice D:I)
            if len(row) >= 6:
                lat = row[4].strip()
                lon = row[5].strip()
                if lat and lon and lat != "0" and lon != "0":
                    existing_coords.add(f"{lat}|{lon}")
                    
        return existing_keys, existing_coords
    except Exception as e:
        print(f"[!] Gagal membaca data lama untuk cek duplikat: {e}")
        return set(), set()
    
async def proses_pencarian_leads_bisnis(data_pencarian_untuk_ai, spreadsheet_id, target_sheet_name="Data Utama"):
    if not sheets_client:
        print("[!] Client Google Sheets tidak aktif. Proses dibatalkan.")
        return

    print(f"\n──────────────────────────────────────")
    print(f"[-] MEMULAI PROSES GOOGLE MAPS WEB SCRAPER (NO API KEY REQUIRED)")
    print(f"──────────────────────────────────────")

    # 1. Ambil data duplikat yang ada di sheet saat ini
    existing_keys, existing_coords = await asyncio.to_thread(get_existing_data, spreadsheet_id, target_sheet_name)
    print(f"[*] Menemukan {len(existing_keys)} nama unik dan {len(existing_coords)} koordinat di sheet.")

    current_client, current_api_key = await client_rotator.get_client()
    valid_items_to_analyze = []
    temp_leads_file = f"temp_leads_to_analyze_{int(time.time())}.csv"
    
    try:
        def create_temp_leads_csv():
            lookup_items = []
            with open(temp_leads_file, 'w', encoding='utf-8', newline='') as f_out:
                writer = csv.writer(f_out)
                writer.writerow(["ID_Entitas", "Komoditas", "Status_Pasar", "Kota", "Negara"])
                counter = 1
                
                data_list = data_pencarian_untuk_ai if isinstance(data_pencarian_untuk_ai, list) else []
                
                for entitas in data_list:
                    komoditas = entitas.get("Komoditas", "").strip() 
                    jenis_stakeholder = entitas.get("Jenis StakeHolder", "").strip()
                    negara = entitas.get("Negara", "").strip() 
                    kota = entitas.get("Kota", "").strip() 
                    prompt_user = entitas.get("Perintah", "").strip() 
                    
                    if not komoditas or not jenis_stakeholder: 
                        continue
                        
                    # Menangani berbagai jenis stakeholder secara dinamis
                    stakeholder_lower = jenis_stakeholder.lower()
                    if stakeholder_lower == "pembeli":
                        status_pasar = "Demand"
                        stakeholder_label = "Pembeli"
                    elif stakeholder_lower == "penjual" or stakeholder_lower == "supplier":
                        status_pasar = "Supply"
                        stakeholder_label = "Supplier"
                    else:
                        # Untuk stakeholder lain seperti Forwarder, Bea Cukai, Agen, dll
                        status_pasar = jenis_stakeholder.title()
                        stakeholder_label = jenis_stakeholder.title()

                    writer.writerow([counter, komoditas, status_pasar, kota, negara, prompt_user]) 
                    
                    lookup_items.append({
                        "id_entitas": counter,
                        "komoditas": komoditas.title(),
                        "stakeholder": stakeholder_label,
                        "negara": negara,
                        "kota": kota,
                        "prompt_user": prompt_user,
                    })
                    counter += 1
            return lookup_items
        valid_items_to_analyze = await asyncio.to_thread(create_temp_leads_csv)
    except Exception as err:
        print(f"[!] Gagal mempersiapkan file CSV lokal: {err}")
        return

    uploaded_leads_file = None
    queries_lookup = {}

    try:
        def upload_leads_to_ai():
            return current_client.files.upload(file=temp_leads_file, config=types.UploadFileConfig(mime_type="text/csv"))
        uploaded_leads_file = await asyncio.to_thread(upload_leads_to_ai)

        # 2. PROMPT BATCH AI: DIPERBARUI UNTUK MENDUKUNG STAKEHOLDER LAINNYA
        prompt_batch_query = """
        Bertindaklah sebagai B2B Lead Generation Specialist & Market Intelligence Internasional.
        Saya adalah seorang eksportir. Saya ingin melihat demand maupun supply dari suatu komoditas.
        Tugas Anda adalah merumuskan TIGA (3) kueri pencarian lokal spesifik (Bahasa Inggris atau lokal) untuk dimasukkan ke Google Maps berdasarkan file CSV yang dilampirkan.

        TARGET STRATEGI STRUKTUR TIER KUERI (WAJIB PATUH):
        - Jika 'status_pasar' merupakan Demand (Pembeli), pecah kueri berdasarkan 3 tingkatan skala bisnis dari kecil ke besar:
          * Kueri 1 (Tier 1 - Skala Kecil / Konsumen Ritel Komersial): Fokus mencari bisnis pengguna akhir yang langsung menyerap produk (contoh: Kafe, Roastery lokal, Bakery, Restoran lokal, dan sebagainya).
          * Kueri 2 (Tier 2 - Skala Menengah / Grosir & Distributor): Fokus mencari rantai distribusi tengah yg berhubungan dengan produk (contoh: B2B Wholesaler, local supplier, distributor bahan baku, dan sebagainya).
          * Kueri 3 (Tier 3 - Skala Besar / Importir & Industri Manufaktur): Fokus mencari penyerap volume masif produk (contoh: Main Importer, Trading House internasional, F&B factory, dan sebagainya).
          
        - Jika 'status_pasar' merupakan Supply (Supplier/Penjual), pecah kueri berdasarkan tingkatan pasokan:
          * Kueri 1 (Tier 1 - Pengrajin/Produsen Kecil): Pembuat lokal, workshop, asosiasi petani lokal atau sebagainya.
          * Kueri 2 (Tier 2 - Pabrik/Supplier Menengah): Supplier B2B lokal, pabrikasi wilayah, processing mill menengah atau sebagainya.
          * Kueri 3 (Tier 3 - Pabrik Besar/Eksportir Utama): Pabrik manufaktur utama skala industri, Perusahaan perdagangan ekspor atau sebagainya.

        - Jika 'status_pasar' merupakan entitas lain (contoh: Forwarder, Bea Cukai, Agen Logistik, dll), pecah kueri berdasarkan jangkauan atau skala operasi:
          * Kueri 1 (Tier 1 - Skala Lokal/Cabang): Kantor cabang lokal, perantara logistik kecil, atau jasa custom clearance perorangan/lokal.
          * Kueri 2 (Tier 2 - Skala Menengah/Nasional): Perusahaan forwarder/logistik skala nasional atau perusahaan B2B kepabeanan.
          * Kueri 3 (Tier 3 - Skala Besar/Pusat/Internasional): Otoritas pelabuhan utama (Port Authority), instansi resmi Bea Cukai pusat (Customs Office), atau perusahaan logistik multinasional.

        Respons HARUS berupa JSON Array murni berisi list objek per id_entitas (tanpa markdown ```json, tanpa penjelasan teks pembuka/penutup):
        [
          {
            "id_entitas": 1,
            "search_targets": [
              { "tipe_bisnis": "Konsumen Hilir (Cafe/Roastery)", "maps_search_query": "Specialty Coffee Cafe Kuala Lumpur" },
              { "tipe_bisnis": "Distributor/Grosir", "maps_search_query": "Coffee Wholesaler Supplier Kuala Lumpur" },
              { "tipe_bisnis": "Importir/Pabrik Besar", "maps_search_query": "Coffee Importer Processing Factory Malaysia" }
            ]
          }
        ]
        """
        batch_query_schema = types.Schema(
            type=types.Type.ARRAY,
            items=types.Schema(
                type=types.Type.OBJECT,
                properties={
                    "id_entitas": types.Schema(type=types.Type.INTEGER),
                    "search_targets": types.Schema(
                        type=types.Type.ARRAY,
                        items=types.Schema(
                            type=types.Type.OBJECT,
                            properties={
                                "tipe_bisnis": types.Schema(type=types.Type.STRING),
                                "maps_search_query": types.Schema(type=types.Type.STRING)
                            },
                            required=["tipe_bisnis", "maps_search_query"]
                        )
                    )
                },
                required=["id_entitas", "search_targets"]
            )
        )

        models_fallback_order = ['gemini-3.1-flash-lite', 'gemma-4-31b-it', 'gemini-3.1-flash-lite', 'gemini-3.5-flash', 'gemma-4-26b-a4b-it']
        response_text = ""
        for model_name in models_fallback_order:
            await gemini_limiter.acquire()
            try:
                print(f"[-] Merumuskan perluasan kueri Google Maps dengan model: {model_name}...")
                response = await current_client.aio.models.generate_content(
                    model=model_name, contents=[uploaded_leads_file, prompt_batch_query],
                    config=types.GenerateContentConfig(response_mime_type="application/json", response_schema=batch_query_schema, temperature=0.1)
                )
                if response and response.text:
                    response_text = response.text.strip()
                    break
            except Exception as e: pass

        match = re.search(r'\[.*\]', response_text, re.DOTALL)
        if match: response_text = match.group(0)
        parsed_queries = json.loads(response_text)
        queries_lookup = {item["id_entitas"]: item["search_targets"] for item in parsed_queries}
    except Exception as err:
        print(f"[!] Gagal memproses File API: {err}")
        return
    finally:
        if uploaded_leads_file:
            try: current_client.files.delete(name=uploaded_leads_file.name)
            except Exception: pass
        if os.path.exists(temp_leads_file): os.remove(temp_leads_file)

    # 3. LIVE GOOGLE MAPS WEB SCRAPING VIA PLAYWRIGHT
    values_to_append = []
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled", "--no-sandbox"])
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0 Safari/537.36")
        page = await context.new_page()

        for item in valid_items_to_analyze:
            search_targets = queries_lookup.get(item["id_entitas"], [])
            
            for target in search_targets:
                maps_query = target.get("maps_search_query", "").strip()
                tipe_bisnis_target = target.get("tipe_bisnis", "").strip()
                if not maps_query: continue

                print(f"    [*] Membuka Google Maps Web -> Kueri: '{maps_query}'...")
                jumlah_per_kueri = 0

                try:
                    encoded_q = urllib.parse.quote_plus(maps_query)
                    maps_url = f"https://www.google.com/maps/search/{encoded_q}?hl=id"
                    
                    await page.goto(maps_url, timeout=30000, wait_until="domcontentloaded")
                    await page.wait_for_timeout(2000)

                    scrollable_sidebar_selector = "div[role='feed']"
                    
                    try:
                        await page.wait_for_selector(scrollable_sidebar_selector, timeout=15000)
                        
                        for scroll_step in range(4):
                            sidebar_element = await page.query_selector(scrollable_sidebar_selector)
                            if sidebar_element:
                                box = await sidebar_element.bounding_box()
                                if box:
                                    await page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
                                    await page.mouse.wheel(0, 3000)
                            
                            await page.wait_for_timeout(1500)
                    except Exception as scroll_err:
                        print(f"        [!] Peringatan kendala scrolling sidebar: {scroll_err}")
                    
                    places_elements = await page.query_selector_all("a[href*='/maps/place/']")
                    
                    if not places_elements:
                        print("        [?] Tautan lokasi fisik a[href*='/maps/place/'] belum termuat di sidebar.")
                        continue
                        
                    print(f"        [+] Menemukan {len(places_elements)} profil bisnis terdaftar resmi. Mengekstrak data...")
                    
                    for link_element in places_elements: 
                        place_name = await link_element.get_attribute("aria-label")
                        place_url = await link_element.get_attribute("href")
                        
                        if not place_name or not place_url:
                            continue
                        
                        # ========================================================
                        # 1. CEK DUPLIKAT NAMA & KOTA LEBIH AWAL (OPTIMASI)
                        # ========================================================
                        current_key = f"{place_name.strip().lower()}|{item['kota'].strip().lower()}"
                        if current_key in existing_keys:
                            continue 

                        existing_keys.add(current_key) 
                        
                        # ========================================================
                        # 2. PROSES PARSING & JITTERING KOORDINAT (~100 METER)
                        # ========================================================
                        lat_val, lon_val = "0", "0"
                        coord_match = re.search(r'!3d([-.\d]+)!4d([-.\d]+)', place_url)
                        if coord_match:
                            try:
                                lat_float = float(coord_match.group(1))
                                lon_float = float(coord_match.group(2))
                                
                                current_coord_str = f"{lat_float:.6f}|{lon_float:.6f}"
                                
                                hitung_geser = 1
                                while current_coord_str in existing_coords:
                                    delta_lat = (0.0009 + random.uniform(-0.0001, 0.0001)) * random.choice([-1, 1])
                                    delta_lon = (0.0009 + random.uniform(-0.0001, 0.0001)) * random.choice([-1, 1])
                                    
                                    lat_geser = lat_float + (delta_lat * hitung_geser)
                                    lon_geser = lon_float + (delta_lon * hitung_geser)
                                    
                                    current_coord_str = f"{lat_geser:.6f}|{lon_geser:.6f}"
                                    hitung_geser += 1
                                
                                lat_val, lon_val = current_coord_str.split("|")
                                existing_coords.add(current_coord_str)
                                
                            except Exception as e:
                                lat_val = coord_match.group(1)
                                lon_val = coord_match.group(2)
                            
                        # Blok duplikat pengecekan nama di bawah ini sudah dihapus
                        kategori_detail = tipe_bisnis_target 
                        
                        try:
                            parent_article = await link_element.query_selector("xpath=ancestor::div[@role='article']")
                            if parent_article:
                                desc_element = await parent_article.query_selector(".fontBodyMedium div:nth-child(4) div:nth-child(1)")
                                if desc_element:
                                    text_kategori = await desc_element.inner_text()
                                    if text_kategori and len(text_kategori.strip()) > 1:
                                        kategori_detail = text_kategori.strip()
                        except Exception:
                            pass

                        acak_4_char = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
                        id_unik = f"UD{acak_4_char}" 

                        row_data = [
                            id_unik,
                            str(item["stakeholder"]),
                            str(item["komoditas"]),
                            str(place_name),
                            str(item["negara"]),
                            str(item["kota"]),
                            str(place_url),
                            f"{lat_val}",
                            f"{lon_val}",
                            "",
                            "",
                            f"{kategori_detail}",
                            f"Profil Usaha Fisik ({kategori_detail}) Terdaftar Resmi di Google Maps wilayah {item['kota']}, {item['negara']}.",
                            str(maps_query)
                        ]
                        values_to_append.append(row_data)
                        jumlah_per_kueri += 1

                    print(f"        [√] Sukses mengekstrak {jumlah_per_kueri} profil untuk kueri ini.")
                    
                except Exception as maps_err:
                    print(f"        [!] Gagal scraping Google Maps Web pada kueri ini: {maps_err}")
                    continue
                    
                await page.wait_for_timeout(2000)

        await browser.close()

    # 4. PUSH DATA BERSIH KE GOOGLE SPREADSHEET
    if not values_to_append:
        print("[!] Tidak ada profil toko fisik/leads yang berhasil lolos filter untuk ditulis.")
        return

    CHUNK_SIZE = 50       
    MAX_RETRIES = 3       
    RETRY_DELAY = 5       
    
    total_data = len(values_to_append)
    print(f"\n[-] Memulai penyimpanan {total_data} leads ke Spreadsheet '{target_sheet_name}'...")
    print(f"[-] Sistem akan memecah pengiriman menjadi batch berukuran {CHUNK_SIZE} baris.")

    total_baris_berhasil = 0

    for i in range(0, total_data, CHUNK_SIZE):
        chunk = values_to_append[i:i + CHUNK_SIZE]
        batch_num = (i // CHUNK_SIZE) + 1
        
        def append_chunk_to_google():
            body = {'values': chunk}
            return sheets_client.values().append(
                spreadsheetId=spreadsheet_id,
                range=f"{target_sheet_name}!A:A",
                valueInputOption="USER_ENTERED",
                body=body
            ).execute()

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                result = await asyncio.to_thread(append_chunk_to_google)
                updates = result.get('updates', {})
                baris_terupdate = updates.get('updatedRows', 0)
                
                print(f"    [+] BATCH {batch_num}: Berhasil menulis {baris_terupdate} baris.")
                total_baris_berhasil += baris_terupdate
                break  

            except Exception as sheet_err:
                print(f"    [!] BATCH {batch_num} - Percobaan {attempt} Gagal: {sheet_err}")
                
                if attempt < MAX_RETRIES:
                    print(f"        [-] Menunggu {RETRY_DELAY} detik sebelum mencoba lagi...")
                    await asyncio.sleep(RETRY_DELAY)
                else:
                    print(f"    [!] BATCH {batch_num} Gagal total setelah {MAX_RETRIES} percobaan. Dilewati.")
        
        if i + CHUNK_SIZE < total_data:
            await asyncio.sleep(2) 

    print(f"\n[+] REPOT MAPS FINAL: Secara keseluruhan berhasil menulis {total_baris_berhasil} dari {total_data} leads ke sheet '{target_sheet_name}'!")
        
# ==========================================
# MAIN ROUTINE
# ==========================================
async def main():
    if not WEB_APP_SCRIPT_URL:
        print("[!] Error: Variabel WEB_APP_SCRIPT_URL belum dikonfigurasi di .env!")
        return
    
    # [1] Memuat data dinamis di awal program
    # [1] Memuat data dinamis di awal program
    DATA_PENCARIAN_MANUAL = await fetch_dynamic_config(WEB_APP_SCRIPT_URL)
    
    if not DATA_PENCARIAN_MANUAL:
        print("[!] Program dihentikan karena kegagalan pemuatan konfigurasi pencarian manual Spreadsheet.")
        return

    # [2] Meneruskan data hasil analisis ke proses pencarian leads bisnis
    # [2] Meneruskan data hasil analisis ke proses pencarian leads bisnis
    SPREADSHEET_ID = os.getenv("GOOGLE_SHEET_ID")
    if not SPREADSHEET_ID:
        print("[!] Error: Variabel GOOGLE_SHEET_ID belum dikonfigurasi di .env!")
        return
    
    print("\n[-] Melanjutkan ke pencarian leads bisnis berdasarkan hasil analisis...")
    await proses_pencarian_leads_bisnis(
        data_pencarian_untuk_ai=DATA_PENCARIAN_MANUAL,
        spreadsheet_id=SPREADSHEET_ID,
        target_sheet_name="Data Utama"
    )

if __name__ == "__main__":
    asyncio.run(main())