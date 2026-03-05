"""
StayFlix Analytics — Plateforme d'intelligence analytique
Netflix × Hôtel | ENSEA 2025-2026
"""
import os, io, sqlite3, hashlib, smtplib
from email.mime.text import MIMEText
from functools import wraps
from datetime import date

from flask import (Flask, render_template, request, redirect, url_for,
                   session, g, flash, send_file, jsonify)
import pandas as pd

app = Flask(__name__)
app.secret_key = 'stayflix_analytics_ensea_2026_X9k!mP#vQ'
app.jinja_env.globals.update(enumerate=enumerate, min=min, max=max,
                              round=round, int=int, abs=abs, len=len, str=str, zip=zip)

DATABASE  = os.path.join(os.path.dirname(__file__), 'stayflix.db')
NETFLIX_F = os.path.join(os.path.dirname(__file__), 'data', 'netflix_titles.xlsx')
HOTEL_F   = os.path.join(os.path.dirname(__file__), 'data', 'hotel_revenue_historical_full.csv')

MAIL_USER = 'kabambadolly5@gmail.com'
MAIL_PASS = 'pulr ijwf kbnr jvwn'

ROLES = {
    'admin':   {'label': 'Administrateur', 'color': '#ef4444', 'access': 'full'},
    'manager': {'label': 'Gestionnaire',   'color': '#f59e0b', 'access': 'full'},
    'analyst': {'label': 'Analyste',       'color': '#60a5fa', 'access': 'no_export'},
    'viewer':  {'label': 'Observateur',    'color': '#94a3b8', 'access': 'limited'},
}

# ISO3 → (lat, lon, nom pays)
ISO3 = {
    'PRT':(39.4,-8.2,'Portugal'),'ESP':(40.5,-3.7,'Espagne'),'GBR':(55.4,-3.4,'Royaume-Uni'),
    'FRA':(46.2,2.2,'France'),'ITA':(41.9,12.6,'Italie'),'DEU':(51.2,10.5,'Allemagne'),
    'IRL':(53.1,-7.7,'Irlande'),'BEL':(50.5,4.5,'Belgique'),'NLD':(52.1,5.3,'Pays-Bas'),
    'USA':(37.1,-95.7,'États-Unis'),'BRA':(-14.2,-51.9,'Brésil'),'CHE':(46.8,8.2,'Suisse'),
    'CN':(35.9,104.2,'Chine'),'CHN':(35.9,104.2,'Chine'),'SWE':(60.1,18.6,'Suède'),
    'ISR':(31.0,34.9,'Israël'),'AUT':(47.5,14.6,'Autriche'),'RUS':(61.5,105.3,'Russie'),
    'POL':(51.9,19.1,'Pologne'),'ROU':(45.9,24.9,'Roumanie'),'AGO':(-11.2,17.9,'Angola'),
    'NOR':(60.5,8.5,'Norvège'),'FIN':(61.9,25.7,'Finlande'),'DNK':(56.3,9.5,'Danemark'),
    'LUX':(49.8,6.1,'Luxembourg'),'CZE':(49.8,15.5,'Tchéquie'),'AUS':(-25.3,133.8,'Australie'),
    'TUR':(38.9,35.2,'Turquie'),'DZA':(28.0,1.7,'Algérie'),'GRC':(39.1,21.8,'Grèce'),
    'MAR':(31.8,-7.1,'Maroc'),'ZAF':(-30.6,22.9,'Afrique du Sud'),'ARG':(-38.4,-63.6,'Argentine'),
    'MEX':(23.6,-102.6,'Mexique'),'JPN':(36.2,138.3,'Japon'),'KOR':(35.9,127.8,'Corée du Sud'),
    'IND':(20.6,79.0,'Inde'),'CAN':(56.1,-106.3,'Canada'),'NGA':(9.1,8.7,'Nigéria'),
    'SGP':(1.4,103.8,'Singapour'),'HKG':(22.3,114.2,'Hong Kong'),'UKR':(48.4,31.2,'Ukraine'),
    'HUN':(47.2,19.5,'Hongrie'),'COL':(4.6,-74.3,'Colombie'),
}

# ─── DB ───────────────────────────────────────────────────────────────────────
def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row
    return g.db

@app.teardown_appcontext
def close_db(e=None):
    db = g.pop('db', None)
    if db: db.close()

def hp(p): return hashlib.sha256(p.encode()).hexdigest()

def init_db():
    db = sqlite3.connect(DATABASE)
    db.executescript('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL, password TEXT NOT NULL,
            first_name TEXT DEFAULT '', last_name TEXT DEFAULT '',
            email TEXT DEFAULT '', gender TEXT DEFAULT 'M',
            role TEXT DEFAULT 'viewer', photo TEXT DEFAULT 'default.png',
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS contacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT, email TEXT, subject TEXT, message TEXT,
            is_read INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS search_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER, dataset TEXT DEFAULT 'netflix',
            query TEXT, results INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    for u in [
        ('admin',   hp('AS3admin2026'), 'Admin',  'System',  'admin@stayflix.ci',   'M', 'admin'),
        ('manager', hp('Manager@2026'), 'Sophie', 'Manager', 'manager@stayflix.ci', 'F', 'manager'),
        ('analyst', hp('Analyst@2026'), 'Adjoua', 'Analyst', 'analyst@stayflix.ci', 'F', 'analyst'),
        ('viewer',  hp('Viewer@2026'),  'Jean',   'Viewer',  'viewer@stayflix.ci',  'M', 'viewer'),
    ]:
        if not db.execute('SELECT id FROM users WHERE username=?', (u[0],)).fetchone():
            db.execute('INSERT INTO users (username,password,first_name,last_name,email,gender,role) VALUES (?,?,?,?,?,?,?)', u)
    db.commit(); db.close()

# ─── AUTH ─────────────────────────────────────────────────────────────────────
def login_required(f):
    @wraps(f)
    def dec(*a, **kw):
        if 'user_id' not in session:
            flash('Connexion requise.', 'warning')
            return redirect(url_for('login'))
        return f(*a, **kw)
    return dec

def roles_required(*roles):
    def deco(f):
        @wraps(f)
        def dec(*a, **kw):
            if 'user_id' not in session: return redirect(url_for('login'))
            if session.get('role') not in roles:
                flash('⛔ Accès restreint — niveau insuffisant.', 'danger')
                return redirect(url_for('home'))
            return f(*a, **kw)
        return dec
    return deco

# ─── DATA LOADERS ─────────────────────────────────────────────────────────────
_netflix, _hotel = None, None

def get_netflix():
    global _netflix
    if _netflix is None:
        try:
            df = pd.read_excel(NETFLIX_F)
            df['release_year'] = pd.to_numeric(df['release_year'], errors='coerce')
            df = df[df['release_year'].between(1900, 2025)]
            df['duration_minutes'] = pd.to_numeric(df['duration_minutes'], errors='coerce')
            df['duration_seasons'] = pd.to_numeric(df['duration_seasons'], errors='coerce')
            if df['date_added'].dtype == object:
                df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')
            df['year_added']  = df['date_added'].dt.year
            df['month_added'] = df['date_added'].dt.month
            df['type'] = df['type'].astype(str)
            _netflix = df
        except Exception as e:
            print(f'Netflix error: {e}'); _netflix = pd.DataFrame()
    return _netflix

def get_hotel():
    global _hotel
    if _hotel is None:
        try:
            df = pd.read_csv(HOTEL_F, sep=';')
            df['adr'] = df['adr'].astype(str).str.replace(',', '.').str.strip()
            df['adr'] = pd.to_numeric(df['adr'], errors='coerce')
            df = df[df['adr'] >= 0]
            df['children'] = df['children'].fillna(0).astype(int)
            df['total_nights'] = df['stays_in_weekend_nights'] + df['stays_in_week_nights']
            df['revenue'] = df['adr'] * df['total_nights']
            mo = ['January','February','March','April','May','June',
                  'July','August','September','October','November','December']
            df['arrival_date_month'] = pd.Categorical(df['arrival_date_month'], categories=mo, ordered=True)
            _hotel = df
        except Exception as e:
            print(f'Hotel error: {e}'); _hotel = pd.DataFrame()
    return _hotel

def safe_int(v):
    try: return int(v)
    except: return 0

def safe_float(v, dec=2):
    try: return round(float(v), dec)
    except: return 0.0

# ─── API: NETFLIX DYNAMIC ─────────────────────────────────────────────────────
@app.route('/api/netflix/stats')
@login_required
def api_netflix_stats():
    df = get_netflix()
    if df.empty: return jsonify({})

    # Filtres
    type_f   = request.args.get('type', '')
    rating_f = request.args.get('rating', '')
    year_f   = request.args.get('year_added', '')
    from_y   = request.args.get('year_from', '')
    to_y     = request.args.get('year_to', '')

    fdf = df.copy()
    if type_f:   fdf = fdf[fdf['type'] == type_f]
    if rating_f: fdf = fdf[fdf['rating'] == rating_f]
    if year_f:   fdf = fdf[fdf['year_added'] == float(year_f)]
    if from_y:   fdf = fdf[fdf['release_year'] >= float(from_y)]
    if to_y:     fdf = fdf[fdf['release_year'] <= float(to_y)]

    tc  = fdf['type'].value_counts()
    rat = fdf['rating'].value_counts().head(10)
    ya  = fdf[fdf['year_added'].notna()].groupby(['year_added', 'type']).size().unstack(fill_value=0).sort_index()
    ry  = fdf['release_year'].value_counts().sort_index()
    ry  = ry[ry.index >= 1990]

    movies = fdf[fdf['type'] == 'Movie']
    shows  = fdf[fdf['type'] == 'TV Show']

    # Distribution durée
    bins  = [0, 60, 90, 120, 150, 180, 9999]
    blabs = ['<60m','60-90m','90-120m','120-150m','150-180m','>180m']
    movies2 = movies.copy()
    movies2['dr'] = pd.cut(movies2['duration_minutes'], bins=bins, labels=blabs)
    dr = movies2['dr'].value_counts().reindex(blabs).fillna(0)

    return jsonify({
        'total':  len(fdf), 'movies': safe_int((fdf['type']=='Movie').sum()),
        'shows':  safe_int((fdf['type']=='TV Show').sum()),
        'avg_duration': safe_float(movies['duration_minutes'].dropna().mean(), 1),
        'avg_seasons':  safe_float(shows['duration_seasons'].dropna().mean(), 1),
        'type_labels': tc.index.tolist(), 'type_counts': tc.values.tolist(),
        'rat_labels': rat.index.tolist(), 'rat_counts': rat.values.tolist(),
        'ya_years':  [safe_int(v) for v in ya.index.tolist()],
        'ya_movies': ya.get('Movie',  pd.Series([0]*len(ya))).tolist(),
        'ya_shows':  ya.get('TV Show', pd.Series([0]*len(ya))).tolist(),
        'ry_years':  [safe_int(v) for v in ry.index.tolist()],
        'ry_counts': ry.values.tolist(),
        'dur_labels': blabs, 'dur_counts': [safe_int(v) for v in dr.values],
        'year_min': safe_int(fdf['release_year'].min()),
        'year_max': safe_int(fdf['release_year'].max()),
        'ratings_count': safe_int(fdf['rating'].nunique()),
        'top10_films': {
            'labels': movies.nlargest(10,'duration_minutes')['title'].str.slice(0,35).tolist() if not movies.empty else [],
            'values': [safe_int(v) for v in movies.nlargest(10,'duration_minutes')['duration_minutes'].tolist()] if not movies.empty else [],
        },
        'top10_shows': {
            'labels': shows.nlargest(10,'duration_seasons')['title'].str.slice(0,35).tolist() if not shows.empty else [],
            'values': [safe_int(v) for v in shows.nlargest(10,'duration_seasons')['duration_seasons'].tolist()] if not shows.empty else [],
        },
    })

# ─── API: HOTEL DYNAMIC ───────────────────────────────────────────────────────
@app.route('/api/hotel/stats')
@login_required
def api_hotel_stats():
    df = get_hotel()
    if df.empty: return jsonify({})

    hotel_f   = request.args.get('hotel', '')
    month_f   = request.args.get('month', '')
    segment_f = request.args.get('segment', '')
    ctype_f   = request.args.get('ctype', '')
    deposit_f = request.args.get('deposit', '')
    cancel_f  = request.args.get('canceled', '')
    adr_min   = request.args.get('adr_min', '')
    adr_max   = request.args.get('adr_max', '')

    fdf = df.copy()
    if hotel_f:   fdf = fdf[fdf['hotel'] == hotel_f]
    if month_f:   fdf = fdf[fdf['arrival_date_month'].astype(str) == month_f]
    if segment_f: fdf = fdf[fdf['market_segment'] == segment_f]
    if ctype_f:   fdf = fdf[fdf['customer_type'] == ctype_f]
    if deposit_f: fdf = fdf[fdf['deposit_type'] == deposit_f]
    if cancel_f != '': fdf = fdf[fdf['is_canceled'] == int(cancel_f)]
    try:
        if adr_min: fdf = fdf[fdf['adr'] >= float(adr_min)]
        if adr_max: fdf = fdf[fdf['adr'] <= float(adr_max)]
    except: pass

    if fdf.empty:
        return jsonify({'total': 0, 'cancel_rate': 0, 'avg_adr': 0, 'revenue': 0})

    ho = fdf['hotel'].value_counts()
    mo = fdf.groupby('arrival_date_month', observed=True).size().sort_index()
    ms = fdf['market_segment'].value_counts().head(7)
    ct = fdf['customer_type'].value_counts()
    dc = fdf['distribution_channel'].value_counts()

    cr_h  = fdf.groupby('hotel')['is_canceled'].mean() * 100
    cr_ms = fdf.groupby('market_segment')['is_canceled'].mean() * 100
    adr_m = fdf.groupby('arrival_date_month', observed=True)['adr'].mean().sort_index()
    rev_m = fdf.groupby('arrival_date_month', observed=True)['revenue'].sum().sort_index()

    # ADR distribution
    adr_bins  = [0, 50, 100, 150, 200, 300, 9999]
    adr_blabs = ['0-50€','50-100€','100-150€','150-200€','200-300€','>300€']
    fdf2 = fdf.copy()
    fdf2['adr_range'] = pd.cut(fdf2['adr'], bins=adr_bins, labels=adr_blabs)
    adr_dist = fdf2['adr_range'].value_counts().reindex(adr_blabs).fillna(0)

    # Nuits distribution
    nb = [0,1,2,3,5,7,9999]; nl = ['0 nuit','1','2','3','4-5','6+']
    fdf2['nr'] = pd.cut(fdf2['total_nights'], bins=nb, labels=nl)
    nr_dist = fdf2['nr'].value_counts().reindex(nl).fillna(0)

    # Lead time
    lb = [0,7,30,60,90,180,365,9999]; ll = ['0-7j','8-30j','31-60j','61-90j','91-180j','181-365j','>365j']
    fdf2['lr'] = pd.cut(fdf2['lead_time'], bins=lb, labels=ll)
    cr_lt = fdf2.groupby('lr', observed=True)['is_canceled'].mean() * 100

    res_status = fdf['reservation_status'].value_counts()

    return jsonify({
        'total':       len(fdf),
        'canceled':    safe_int(fdf['is_canceled'].sum()),
        'cancel_rate': safe_float(fdf['is_canceled'].mean() * 100, 1),
        'avg_adr':     safe_float(fdf['adr'].mean()),
        'max_adr':     safe_float(fdf['adr'].max()),
        'revenue':     safe_float(fdf['revenue'].sum(), 0),
        'avg_nights':  safe_float(fdf['total_nights'].mean(), 1),
        'avg_lead':    safe_int(fdf['lead_time'].mean()),
        'repeat_pct':  safe_float(fdf['is_repeated_guest'].mean() * 100, 1),
        'countries':   safe_int(fdf['country'].nunique()),
        'resort_count': safe_int((fdf['hotel']=='Resort Hotel').sum()),
        'city_count':   safe_int((fdf['hotel']=='City Hotel').sum()),
        'hotel_labels': ho.index.tolist(), 'hotel_counts': ho.values.tolist(),
        'mo_labels':  [str(m) for m in mo.index], 'mo_counts': mo.values.tolist(),
        'ms_labels':  ms.index.tolist(), 'ms_counts': ms.values.tolist(),
        'ct_labels':  ct.index.tolist(), 'ct_counts': ct.values.tolist(),
        'dc_labels':  dc.index.tolist(), 'dc_counts': dc.values.tolist(),
        'crh_labels': cr_h.index.tolist(), 'crh_values': cr_h.round(1).values.tolist(),
        'crms_labels': cr_ms.sort_values(ascending=False).index.tolist(),
        'crms_values': cr_ms.sort_values(ascending=False).round(1).values.tolist(),
        'adrm_labels': [str(m) for m in adr_m.index], 'adrm_values': adr_m.round(2).values.tolist(),
        'revm_labels': [str(m) for m in rev_m.index], 'revm_values': rev_m.round(0).astype(int).values.tolist(),
        'adr_dist_labels': adr_blabs, 'adr_dist_counts': [safe_int(v) for v in adr_dist.values],
        'nr_labels': nl, 'nr_counts': [safe_int(v) for v in nr_dist.values],
        'crlt_labels': ll, 'crlt_values': [safe_float(cr_lt.get(l, 0), 1) for l in ll],
        'rs_labels': res_status.index.tolist(), 'rs_counts': res_status.values.tolist(),
    })

# ─── API: MAP ─────────────────────────────────────────────────────────────────
@app.route('/api/hotel/map')
@login_required
def api_hotel_map():
    df = get_hotel()
    if df.empty: return jsonify([])

    hotel_f   = request.args.get('hotel', '')
    month_f   = request.args.get('month', '')
    cancel_f  = request.args.get('canceled', '')
    segment_f = request.args.get('segment', '')

    fdf = df.copy()
    if hotel_f:   fdf = fdf[fdf['hotel'] == hotel_f]
    if month_f:   fdf = fdf[fdf['arrival_date_month'].astype(str) == month_f]
    if cancel_f != '': fdf = fdf[fdf['is_canceled'] == int(cancel_f)]
    if segment_f: fdf = fdf[fdf['market_segment'] == segment_f]

    top = fdf['country'].value_counts().head(35)
    result = []
    for code, count in top.items():
        if code in ISO3:
            lat, lon, name = ISO3[code]
            sub = fdf[fdf['country'] == code]
            result.append({
                'country': name, 'code': code, 'count': safe_int(count),
                'lat': lat, 'lon': lon,
                'cancel': safe_float(sub['is_canceled'].mean() * 100, 1),
                'adr':    safe_float(sub['adr'].mean()),
                'revenue': safe_float(sub['revenue'].sum(), 0),
                'nights':  safe_float(sub['total_nights'].mean(), 1),
            })
    return jsonify(result)

# ─── ROUTES PUBLIQUES ─────────────────────────────────────────────────────────
@app.route('/')
def index():
    return redirect(url_for('home') if session.get('user_id') else url_for('login'))

@app.route('/home')
@login_required
def home():
    dn = get_netflix(); dh = get_hotel()
    ns = {}; hs = {}
    if not dn.empty:
        ns = {'total': len(dn), 'movies': safe_int((dn['type']=='Movie').sum()),
              'shows': safe_int((dn['type']=='TV Show').sum()), 'ratings': dn['rating'].nunique(),
              'year_min': safe_int(dn['release_year'].min()), 'year_max': safe_int(dn['release_year'].max()),
              'avg_duration': safe_float(dn[dn['type']=='Movie']['duration_minutes'].dropna().mean(), 1),
              'avg_seasons':  safe_float(dn[dn['type']=='TV Show']['duration_seasons'].dropna().mean(), 1)}
    if not dh.empty:
        hs = {'total': len(dh), 'cancel_rate': safe_float(dh['is_canceled'].mean()*100, 1),
              'avg_adr': safe_float(dh['adr'].mean()), 'countries': safe_int(dh['country'].nunique()),
              'resort_count': safe_int((dh['hotel']=='Resort Hotel').sum()),
              'city_count':   safe_int((dh['hotel']=='City Hotel').sum()),
              'revenue': safe_float(dh['revenue'].sum(), 0),
              'repeat_pct': safe_float(dh['is_repeated_guest'].mean()*100, 1)}
    return render_template('home.html', ns=ns, hs=hs)

@app.route('/login', methods=['GET','POST'])
def login():
    if session.get('user_id'): return redirect(url_for('home'))
    error = None
    if request.method == 'POST':
        u, p = request.form.get('username','').strip(), request.form.get('password','').strip()
        qr = request.form.get('quick_role','')
        if qr:
            qm = {'admin':('admin','AS3admin2026'),'manager':('manager','Manager@2026'),
                  'analyst':('analyst','Analyst@2026'),'viewer':('viewer','Viewer@2026')}
            if qr in qm: u, p = qm[qr]
        user = get_db().execute(
            'SELECT * FROM users WHERE username=? AND password=? AND is_active=1',
            (u, hp(p))).fetchone()
        if user:
            session.update({'user_id':user['id'],'username':user['username'],'role':user['role'],
                            'first_name':user['first_name'],'last_name':user['last_name'],'photo':user['photo']})
            lbl = ROLES[user['role']]['label']
            flash(f"Bienvenue {user['first_name']} — {lbl} ✓", 'success')
            return redirect(url_for('home'))
        error = 'Identifiants incorrects.'
    return render_template('login.html', error=error)

@app.route('/register', methods=['GET','POST'])
def register():
    if session.get('user_id'): return redirect(url_for('home'))
    if request.method == 'POST':
        u  = request.form.get('username','').strip()
        fn = request.form.get('first_name','').strip()
        ln = request.form.get('last_name','').strip()
        em = request.form.get('email','').strip()
        pw = request.form.get('password','').strip()
        p2 = request.form.get('password2','').strip()
        if not all([u,fn,ln,em,pw]):
            flash('Tous les champs sont requis.', 'danger'); return redirect(url_for('register'))
        if pw != p2:
            flash('Mots de passe non identiques.', 'danger'); return redirect(url_for('register'))
        if len(pw) < 8:
            flash('Minimum 8 caractères requis.', 'danger'); return redirect(url_for('register'))
        try:
            get_db().execute('INSERT INTO users (username,password,first_name,last_name,email,role) VALUES (?,?,?,?,?,?)',
                             (u, hp(pw), fn, ln, em, 'viewer'))
            get_db().commit()
            flash('Compte créé ! Connectez-vous.', 'success')
            return redirect(url_for('login'))
        except sqlite3.IntegrityError:
            flash("Nom d'utilisateur déjà pris.", 'danger'); return redirect(url_for('register'))
    return render_template('register.html')

@app.route('/logout')
def logout():
    session.clear(); flash('Déconnexion réussie.', 'info')
    return redirect(url_for('login'))

@app.route('/contact', methods=['GET','POST'])
@login_required
def contact():
    if request.method == 'POST':
        name, email, subject, message = (request.form.get(k,'').strip() for k in ['name','email','subject','message'])
        if not all([name,email,subject,message]):
            flash('Tous les champs requis.', 'danger'); return redirect(url_for('contact'))
        get_db().execute('INSERT INTO contacts (name,email,subject,message) VALUES (?,?,?,?)',(name,email,subject,message))
        get_db().commit()
        try:
            msg = MIMEText(f"De: {name} <{email}>\n\n{message}")
            msg['Subject'] = f'[StayFlix] {subject}'; msg['From'] = MAIL_USER; msg['To'] = MAIL_USER
            with smtplib.SMTP('smtp.gmail.com', 587) as s:
                s.starttls(); s.login(MAIL_USER, MAIL_PASS); s.send_message(msg)
        except: pass
        flash('✅ Message envoyé !', 'success'); return redirect(url_for('contact'))
    return render_template('contact.html')

# ─── NETFLIX ROUTES ───────────────────────────────────────────────────────────
@app.route('/netflix/dashboard')
@login_required
def netflix_dashboard():
    df = get_netflix()
    s = {}
    if not df.empty:
        tc  = df['type'].value_counts()
        rat = df['rating'].value_counts().head(10)
        ya  = df[df['year_added'].notna()].groupby(['year_added','type']).size().unstack(fill_value=0).sort_index()
        ry  = df['release_year'].value_counts().sort_index()
        ry  = ry[ry.index >= 1990]
        movies = df[df['type']=='Movie']
        shows  = df[df['type']=='TV Show']
        bins  = [0,60,90,120,150,180,9999]
        blabs = ['<60m','60-90m','90-120m','120-150m','150-180m','>180m']
        m2 = movies.copy(); m2['dr'] = pd.cut(m2['duration_minutes'], bins=bins, labels=blabs)
        dr = m2['dr'].value_counts().reindex(blabs).fillna(0)
        s = {
            'total': len(df), 'movies': safe_int((df['type']=='Movie').sum()),
            'shows': safe_int((df['type']=='TV Show').sum()),
            'ratings_count': safe_int(df['rating'].nunique()),
            'avg_duration': safe_float(movies['duration_minutes'].dropna().mean(), 1),
            'avg_seasons':  safe_float(shows['duration_seasons'].dropna().mean(), 1),
            'max_duration': safe_int(movies['duration_minutes'].dropna().max()),
            'max_seasons':  safe_int(shows['duration_seasons'].dropna().max()),
            'year_min': safe_int(df['release_year'].min()), 'year_max': safe_int(df['release_year'].max()),
            'type_labels': tc.index.tolist(), 'type_counts': [int(v) for v in tc.values.tolist()],
            'rat_labels': rat.index.tolist(), 'rat_counts': [int(v) for v in rat.values.tolist()],
            'ya_years':  [safe_int(v) for v in ya.index.tolist()],
            'ya_movies': [int(v) for v in ya.get('Movie', pd.Series([0]*len(ya))).tolist()],
            'ya_shows':  [int(v) for v in ya.get('TV Show', pd.Series([0]*len(ya))).tolist()],
            'ry_years':  [safe_int(v) for v in ry.index.tolist()],
            'ry_counts': [int(v) for v in ry.values.tolist()],
            'dur_labels': blabs, 'dur_counts': [safe_int(v) for v in dr.values],
            'all_ratings': sorted(df['rating'].dropna().unique().tolist()),
            'ratings': safe_int(df['rating'].nunique()),
            'top10_films': {
                'labels': movies.nlargest(10,'duration_minutes')['title'].str.slice(0,35).tolist() if not movies.empty else [],
                'values': [safe_int(v) for v in movies.nlargest(10,'duration_minutes')['duration_minutes'].tolist()] if not movies.empty else [],
                'years':  [safe_int(v) for v in movies.nlargest(10,'duration_minutes')['release_year'].tolist()] if not movies.empty else [],
            },
            'top10_shows': {
                'labels': shows.nlargest(10,'duration_seasons')['title'].str.slice(0,35).tolist() if not shows.empty else [],
                'values': [safe_int(v) for v in shows.nlargest(10,'duration_seasons')['duration_seasons'].tolist()] if not shows.empty else [],
                'years':  [safe_int(v) for v in shows.nlargest(10,'duration_seasons')['release_year'].tolist()] if not shows.empty else [],
            },
        }
    return render_template('netflix/dashboard.html', s=s)

@app.route('/netflix/content')
@login_required
def netflix_content():
    df = get_netflix()
    s = {}
    if not df.empty:
        movies = df[df['type']=='Movie'].copy(); shows = df[df['type']=='TV Show'].copy()
        bins=[0,60,90,120,150,180,9999]; blabs=['<60m','60-90m','90-120m','120-150m','150-180m','>180m']
        movies['dr'] = pd.cut(movies['duration_minutes'], bins=bins, labels=blabs)
        dr = movies['dr'].value_counts().reindex(blabs).fillna(0)
        sc = shows['duration_seasons'].value_counts().sort_index().head(12)
        rat_m = movies['rating'].value_counts().head(8); rat_s = shows['rating'].value_counts().head(8)
        all_r = sorted(set(rat_m.index.tolist() + rat_s.index.tolist()))
        m_y = movies['release_year'].value_counts().sort_index().tail(20)
        s_y = shows['release_year'].value_counts().sort_index().tail(20)
        s = {
            'total_movies': len(movies), 'total_shows': len(shows),
            'avg_duration': safe_float(movies['duration_minutes'].dropna().mean(), 1),
            'max_duration': safe_int(movies['duration_minutes'].dropna().max()),
            'avg_seasons':  safe_float(shows['duration_seasons'].dropna().mean(), 1),
            'max_seasons':  safe_int(shows['duration_seasons'].dropna().max()),
            'dur_labels': blabs, 'dur_counts': [safe_int(v) for v in dr.values],
            'seas_labels': [str(safe_int(v))+'s' for v in sc.index], 'seas_counts': sc.values.tolist(),
            'ratcomp_labels': all_r,
            'ratcomp_movies': [safe_int(rat_m.get(r,0)) for r in all_r],
            'ratcomp_shows':  [safe_int(rat_s.get(r,0)) for r in all_r],
            'myear_labels': [safe_int(v) for v in m_y.index], 'myear_counts': m_y.values.tolist(),
            'syear_labels': [safe_int(v) for v in s_y.index], 'syear_counts': s_y.values.tolist(),
            'all_ratings': sorted(df['rating'].dropna().unique().tolist()),
        }
    return render_template('netflix/content.html', s=s)

@app.route('/netflix/ratings')
@login_required
def netflix_ratings():
    df = get_netflix()
    s = {}
    if not df.empty:
        rat = df['rating'].value_counts()
        groups = {'Tout public':['TV-G','G','TV-Y'],'Enfants':['TV-Y7','TV-Y7-FV','PG'],
                  'Adolescents':['TV-PG','TV-14','PG-13'],'Adultes':['TV-MA','R','NC-17','NR','UR']}
        gd = {g: safe_int(df[df['rating'].isin(c)].shape[0]) for g,c in groups.items()}
        pivot = df.groupby(['rating','type']).size().unstack(fill_value=0)
        top_r = df['rating'].value_counts().head(10).index
        pivot = pivot.reindex(top_r)
        mv = df[df['type']=='Movie']
        dr = mv.groupby('rating')['duration_minutes'].mean().sort_values(ascending=False).head(10)
        top5 = df['rating'].value_counts().head(5).index.tolist()
        df2  = df[df['rating'].isin(top5) & df['year_added'].notna()]
        ev   = df2.groupby(['year_added','rating']).size().unstack(fill_value=0)
        s = {
            'rat_labels': rat.index.tolist(), 'rat_counts': rat.values.tolist(),
            'grp_labels': list(gd.keys()), 'grp_counts': list(gd.values()),
            'pivot_labels': pivot.index.tolist(),
            'pivot_movies': [safe_int(pivot.get('Movie', pd.Series()).get(r,0)) for r in pivot.index],
            'pivot_shows':  [safe_int(pivot.get('TV Show', pd.Series()).get(r,0)) for r in pivot.index],
            'dr_labels': dr.index.tolist(), 'dr_values': dr.round(1).values.tolist(),
            'ev_years': [safe_int(v) for v in ev.index.tolist()],
            'ev_datasets': [{'label':r,'data':ev[r].tolist() if r in ev.columns else []} for r in top5],
            'most_common': rat.index[0], 'most_common_count': safe_int(rat.values[0]),
            'ratings_total': safe_int(df['rating'].nunique()),
        }
    return render_template('netflix/ratings.html', s=s)

@app.route('/netflix/timeline')
@login_required
def netflix_timeline():
    df = get_netflix()
    s = {}
    if not df.empty:
        ya = df[df['year_added'].notna()].groupby(['year_added','type']).size().unstack(fill_value=0).sort_index()
        months_fr=['Janvier','Février','Mars','Avril','Mai','Juin','Juillet','Août','Septembre','Octobre','Novembre','Décembre']
        ma = df[df['month_added'].notna()].groupby('month_added').size().reindex(range(1,13),fill_value=0)
        ry = df['release_year'].value_counts().sort_index(); ry = ry[ry.index>=1990]
        df3 = df[df['year_added'].notna()].copy()
        df3['ecart'] = df3['year_added'] - df3['release_year']
        df3 = df3[df3['ecart'].between(0,30)]
        ec = df3.groupby('ecart').size().head(20)
        ya2 = df[df['year_added'].notna()].groupby('year_added').size().sort_index().cumsum()
        peak_y = safe_int(df[df['year_added'].notna()].groupby('year_added').size().idxmax())
        peak_c = safe_int(df[df['year_added'].notna()].groupby('year_added').size().max())
        s = {
            'ya_years':  [safe_int(v) for v in ya.index.tolist()],
            'ya_movies': ya.get('Movie',  pd.Series([0]*len(ya))).tolist(),
            'ya_shows':  ya.get('TV Show', pd.Series([0]*len(ya))).tolist(),
            'ma_months': months_fr, 'ma_counts': ma.values.tolist(),
            'ry_years':  [safe_int(v) for v in ry.index.tolist()], 'ry_counts': ry.values.tolist(),
            'ec_labels': [f"{safe_int(v)} an{'s' if v>1 else ''}" for v in ec.index],
            'ec_counts': ec.values.tolist(),
            'cum_years': [safe_int(v) for v in ya2.index.tolist()], 'cum_counts': ya2.values.tolist(),
            'first_year': safe_int(df['year_added'].min()), 'latest_year': safe_int(df['year_added'].max()),
            'peak_year': peak_y, 'peak_count': peak_c,
        }
    return render_template('netflix/timeline.html', s=s)

@app.route('/netflix/search')
@login_required
def netflix_search():
    df = get_netflix()
    results = None; qinfo = {}
    type_f  = request.args.get('type','')
    rat_f   = request.args.get('rating','')
    yr_min  = request.args.get('year_min','')
    yr_max  = request.args.get('year_max','')
    dur_min = request.args.get('dur_min','')
    dur_max = request.args.get('dur_max','')
    seas_min= request.args.get('seas_min','')
    title_q = request.args.get('title','').strip()
    desc_q  = request.args.get('desc','').strip()
    ya_from = request.args.get('ya_from','')
    ya_to   = request.args.get('ya_to','')
    sort_by = request.args.get('sort_by','release_year')
    order   = request.args.get('order','desc')
    filters = any([type_f,rat_f,yr_min,yr_max,dur_min,dur_max,seas_min,title_q,desc_q,ya_from,ya_to])
    if not df.empty:
        fdf = df.copy()
        if type_f:   fdf = fdf[fdf['type']==type_f]
        if rat_f:    fdf = fdf[fdf['rating']==rat_f]
        if title_q:  fdf = fdf[fdf['title'].str.contains(title_q, case=False, na=False)]
        if desc_q:   fdf = fdf[fdf['description'].str.contains(desc_q, case=False, na=False)]
        try:
            if yr_min:  fdf = fdf[fdf['release_year']>=float(yr_min)]
            if yr_max:  fdf = fdf[fdf['release_year']<=float(yr_max)]
            if dur_min: fdf = fdf[fdf['duration_minutes']>=float(dur_min)]
            if dur_max: fdf = fdf[fdf['duration_minutes']<=float(dur_max)]
            if seas_min:fdf = fdf[fdf['duration_seasons']>=float(seas_min)]
            if ya_from: fdf = fdf[fdf['year_added']>=float(ya_from)]
            if ya_to:   fdf = fdf[fdf['year_added']<=float(ya_to)]
        except: pass
        v = ['release_year','duration_minutes','year_added','duration_seasons']
        if sort_by not in v: sort_by = 'release_year'
        fdf = fdf.sort_values(sort_by, ascending=(order=='asc'), na_position='last')
        qinfo = {'total':len(fdf),'movies':safe_int((fdf['type']=='Movie').sum()),'shows':safe_int((fdf['type']=='TV Show').sum())}
        results = fdf.head(200)[['title','type','rating','release_year','duration_minutes','duration_seasons','year_added','description']].to_dict(orient='records')
        if filters and 'user_id' in session:
            q = '&'.join(f"{k}={v}" for k,v in request.args.items() if v)
            get_db().execute('INSERT INTO search_history (user_id,dataset,query,results) VALUES (?,?,?,?)',(session['user_id'],'netflix',q,len(fdf)))
            get_db().commit()
    types   = sorted(df['type'].dropna().unique()) if not df.empty else []
    ratings = sorted(df['rating'].dropna().unique()) if not df.empty else []
    return render_template('netflix/search.html', results=results, qinfo=qinfo, filters=filters,
                           types=types, ratings=ratings, current=request.args)

# ─── HOTEL ROUTES ─────────────────────────────────────────────────────────────
@app.route('/hotel/dashboard')
@login_required
def hotel_dashboard():
    df = get_hotel()
    s = {}
    if not df.empty:
        ho = df['hotel'].value_counts()
        mo = df.groupby('arrival_date_month', observed=True).size().sort_index()
        ms = df['market_segment'].value_counts().head(7)
        adr_m = df.groupby('arrival_date_month', observed=True)['adr'].mean().sort_index()
        cr_h  = df.groupby('hotel')['is_canceled'].mean() * 100
        s = {
            'total': len(df), 'canceled': safe_int(df['is_canceled'].sum()),
            'cancel_rate': safe_float(df['is_canceled'].mean()*100, 1),
            'avg_adr': safe_float(df['adr'].mean()), 'max_adr': safe_float(df['adr'].max()),
            'revenue': safe_float(df['revenue'].sum(), 0), 'countries': safe_int(df['country'].nunique()),
            'resort_count': safe_int((df['hotel']=='Resort Hotel').sum()),
            'city_count':   safe_int((df['hotel']=='City Hotel').sum()),
            'avg_nights': safe_float(df['total_nights'].mean(), 1),
            'avg_lead': safe_int(df['lead_time'].mean()),
            'repeat_pct': safe_float(df['is_repeated_guest'].mean()*100, 1),
            'hotel_labels': ho.index.tolist(), 'hotel_counts': ho.values.tolist(),
            'mo_labels': [str(m) for m in mo.index], 'mo_counts': mo.values.tolist(),
            'ms_labels': ms.index.tolist(), 'ms_counts': ms.values.tolist(),
            'adrm_labels': [str(m) for m in adr_m.index], 'adrm_values': adr_m.round(2).values.tolist(),
            'crh_labels': cr_h.index.tolist(), 'crh_values': cr_h.round(1).values.tolist(),
            'all_months': [str(m) for m in df['arrival_date_month'].cat.categories],
            'all_segments': sorted(df['market_segment'].dropna().unique().tolist()),
            'all_ctypes': sorted(df['customer_type'].dropna().unique().tolist()),
            'all_deposits': sorted(df['deposit_type'].dropna().unique().tolist()),
        }
    return render_template('hotel/dashboard.html', s=s)

@app.route('/hotel/bookings')
@login_required
def hotel_bookings():
    df = get_hotel()
    s = {}
    if not df.empty:
        ct = df['customer_type'].value_counts()
        dc = df['distribution_channel'].value_counts()
        rr = df['reserved_room_type'].value_counts().head(8)
        lt = df.groupby('market_segment')['lead_time'].mean().sort_values(ascending=False)
        nb=[0,1,2,3,5,7,9999]; nl=['0 nuit','1','2','3','4-5','6-7']
        df2=df.copy(); df2['nr']=pd.cut(df2['total_nights'],bins=nb,labels=nl)
        nr=df2['nr'].value_counts().reindex(nl).fillna(0)
        df2['room_match'] = (df['reserved_room_type'] == df['assigned_room_type'])
        s = {
            'avg_nights': safe_float(df['total_nights'].mean(),1), 'max_nights': safe_int(df['total_nights'].max()),
            'avg_lead': safe_int(df['lead_time'].mean()), 'repeat_pct': safe_float(df['is_repeated_guest'].mean()*100,1),
            'avg_weekend': safe_float(df['stays_in_weekend_nights'].mean(),1),
            'avg_week':    safe_float(df['stays_in_week_nights'].mean(),1),
            'room_match':  safe_float(df2['room_match'].mean()*100,1),
            'ct_labels': ct.index.tolist(), 'ct_counts': ct.values.tolist(),
            'dc_labels': dc.index.tolist(), 'dc_counts': dc.values.tolist(),
            'rr_labels': rr.index.tolist(), 'rr_counts': rr.values.tolist(),
            'nr_labels': nl, 'nr_counts': [safe_int(v) for v in nr.values],
            'lt_labels': lt.index.tolist(), 'lt_values': lt.round(0).astype(int).values.tolist(),
            'all_months': [str(m) for m in df['arrival_date_month'].cat.categories],
            'all_segments': sorted(df['market_segment'].dropna().unique().tolist()),
            'all_ctypes': sorted(df['customer_type'].dropna().unique().tolist()),
        }
    return render_template('hotel/bookings.html', s=s)

@app.route('/hotel/revenue')
@login_required
@roles_required('admin','manager','analyst')
def hotel_revenue():
    df = get_hotel()
    s = {}
    if not df.empty:
        adr_h  = df.groupby('hotel')['adr'].mean()
        rev_m  = df.groupby('arrival_date_month', observed=True)['revenue'].sum().sort_index()
        adr_ct = df.groupby('customer_type')['adr'].mean().sort_values(ascending=False)
        adr_ms = df.groupby('market_segment')['adr'].mean().sort_values(ascending=False)
        adr_dep= df.groupby('deposit_type')['adr'].mean()
        box = {}
        for h in df['hotel'].unique():
            sub = df[df['hotel']==h]['adr']
            k = h.replace(' ','_').lower()
            box[k]={'min':round(float(sub.min()),2),'q1':round(float(sub.quantile(.25)),2),
                    'med':round(float(sub.median()),2),'q3':round(float(sub.quantile(.75)),2),
                    'max':round(float(sub.max()),2),'mean':round(float(sub.mean()),2)}
        s = {
            'total_rev': safe_float(df['revenue'].sum(),0), 'avg_adr': safe_float(df['adr'].mean()),
            'max_adr': safe_float(df['adr'].max()),
            'resort_adr': safe_float(df[df['hotel']=='Resort Hotel']['adr'].mean()),
            'city_adr':   safe_float(df[df['hotel']=='City Hotel']['adr'].mean()),
            'adrh_labels': adr_h.index.tolist(), 'adrh_values': adr_h.round(2).values.tolist(),
            'revm_labels': [str(m) for m in rev_m.index], 'revm_values': rev_m.round(0).astype(int).values.tolist(),
            'adrct_labels': adr_ct.index.tolist(), 'adrct_values': adr_ct.round(2).values.tolist(),
            'adrms_labels': adr_ms.index.tolist(), 'adrms_values': adr_ms.round(2).values.tolist(),
            'adrdep_labels': adr_dep.index.tolist(), 'adrdep_values': adr_dep.round(2).values.tolist(),
            'box': box,
            'all_months': [str(m) for m in df['arrival_date_month'].cat.categories],
            'all_segments': sorted(df['market_segment'].dropna().unique().tolist()),
        }
    return render_template('hotel/revenue.html', s=s)

@app.route('/hotel/cancellations')
@login_required
def hotel_cancellations():
    df = get_hotel()
    s = {}
    if not df.empty:
        cr_h  = df.groupby('hotel')['is_canceled'].mean()*100
        cr_dep= df.groupby('deposit_type')['is_canceled'].mean()*100
        cr_ms = df.groupby('market_segment')['is_canceled'].mean()*100
        cr_mo = df.groupby('arrival_date_month', observed=True)['is_canceled'].mean()*100
        lb=[0,7,30,60,90,180,365,9999]; ll=['0-7j','8-30j','31-60j','61-90j','91-180j','181-365j','>365j']
        df2=df.copy(); df2['lr']=pd.cut(df2['lead_time'],bins=lb,labels=ll)
        cr_lt = df2.groupby('lr', observed=True)['is_canceled'].mean()*100
        rs = df['reservation_status'].value_counts()
        s = {
            'cancel_rate': safe_float(df['is_canceled'].mean()*100,1),
            'total_canceled': safe_int(df['is_canceled'].sum()),
            'total_confirmed': safe_int((df['is_canceled']==0).sum()),
            'crh_labels': cr_h.index.tolist(), 'crh_values': cr_h.round(1).values.tolist(),
            'crdep_labels': cr_dep.index.tolist(), 'crdep_values': cr_dep.round(1).values.tolist(),
            'crms_labels': cr_ms.sort_values(ascending=False).index.tolist(),
            'crms_values': cr_ms.sort_values(ascending=False).round(1).values.tolist(),
            'crmo_labels': [str(m) for m in cr_mo.sort_index().index],
            'crmo_values': cr_mo.sort_index().round(1).values.tolist(),
            'crlt_labels': ll, 'crlt_values': [safe_float(cr_lt.get(l,0),1) for l in ll],
            'rs_labels': rs.index.tolist(), 'rs_counts': rs.values.tolist(),
            'all_months': [str(m) for m in df['arrival_date_month'].cat.categories],
            'all_segments': sorted(df['market_segment'].dropna().unique().tolist()),
            'all_deposits': sorted(df['deposit_type'].dropna().unique().tolist()),
        }
    return render_template('hotel/cancellations.html', s=s)

@app.route('/hotel/map')
@login_required
def hotel_map():
    df = get_hotel()
    s = {'total': len(df) if not df.empty else 0,
         'countries': safe_int(df['country'].nunique()) if not df.empty else 0}
    months = [str(m) for m in df['arrival_date_month'].cat.categories] if not df.empty else []
    segments = sorted(df['market_segment'].dropna().unique().tolist()) if not df.empty else []
    return render_template('hotel/map.html', s=s, months=months, segments=segments)

@app.route('/hotel/search')
@login_required
def hotel_search():
    df = get_hotel()
    results = None; qinfo = {}
    hotel_f  = request.args.get('hotel','')
    month_f  = request.args.get('month','')
    seg_f    = request.args.get('segment','')
    ctype_f  = request.args.get('ctype','')
    dep_f    = request.args.get('deposit','')
    cancel_f = request.args.get('canceled','')
    adr_min  = request.args.get('adr_min','')
    adr_max  = request.args.get('adr_max','')
    nights_min= request.args.get('nights_min','')
    lead_max = request.args.get('lead_max','')
    country_f= request.args.get('country','').strip().upper()
    sort_by  = request.args.get('sort_by','adr')
    order    = request.args.get('order','desc')
    filters  = any([hotel_f,month_f,seg_f,ctype_f,dep_f,cancel_f,adr_min,adr_max,nights_min,lead_max,country_f])
    if not df.empty:
        fdf = df.copy()
        if hotel_f:   fdf = fdf[fdf['hotel']==hotel_f]
        if month_f:   fdf = fdf[fdf['arrival_date_month'].astype(str)==month_f]
        if seg_f:     fdf = fdf[fdf['market_segment']==seg_f]
        if ctype_f:   fdf = fdf[fdf['customer_type']==ctype_f]
        if dep_f:     fdf = fdf[fdf['deposit_type']==dep_f]
        if cancel_f != '': fdf = fdf[fdf['is_canceled']==int(cancel_f)]
        if country_f: fdf = fdf[fdf['country']==country_f]
        try:
            if adr_min:   fdf = fdf[fdf['adr']>=float(adr_min)]
            if adr_max:   fdf = fdf[fdf['adr']<=float(adr_max)]
            if nights_min:fdf = fdf[fdf['total_nights']>=float(nights_min)]
            if lead_max:  fdf = fdf[fdf['lead_time']<=float(lead_max)]
        except: pass
        v = ['adr','lead_time','total_nights','revenue']
        if sort_by not in v: sort_by = 'adr'
        fdf = fdf.sort_values(sort_by, ascending=(order=='asc'), na_position='last')
        qinfo = {'total':len(fdf), 'avg_adr':safe_float(fdf['adr'].mean()),
                 'cancel_rate':safe_float(fdf['is_canceled'].mean()*100,1) if len(fdf)>0 else 0}
        cols = ['hotel','arrival_date_month','market_segment','customer_type','deposit_type',
                'is_canceled','adr','total_nights','revenue','lead_time','country','room_match' if False else 'reserved_room_type']
        results = fdf.head(200)[['hotel','arrival_date_month','market_segment','customer_type',
                                  'deposit_type','is_canceled','adr','total_nights','revenue',
                                  'lead_time','country','reserved_room_type']].to_dict(orient='records')
        if filters and 'user_id' in session:
            q = '&'.join(f"{k}={v}" for k,v in request.args.items() if v)
            get_db().execute('INSERT INTO search_history (user_id,dataset,query,results) VALUES (?,?,?,?)',(session['user_id'],'hotel',q,len(fdf)))
            get_db().commit()
    hotels   = ['Resort Hotel','City Hotel'] if not df.empty else []
    months   = [str(m) for m in df['arrival_date_month'].cat.categories] if not df.empty else []
    segments = sorted(df['market_segment'].dropna().unique().tolist()) if not df.empty else []
    ctypes   = sorted(df['customer_type'].dropna().unique().tolist()) if not df.empty else []
    deposits = sorted(df['deposit_type'].dropna().unique().tolist()) if not df.empty else []
    return render_template('hotel/search.html', results=results, qinfo=qinfo, filters=filters,
                           hotels=hotels, months=months, segments=segments, ctypes=ctypes,
                           deposits=deposits, current=request.args)

# ─── EXPORTS ──────────────────────────────────────────────────────────────────
@app.route('/export/netflix')
@login_required
@roles_required('admin','manager')
def export_netflix():
    df = get_netflix()
    if df.empty: flash('Aucune donnée.','warning'); return redirect(url_for('netflix_dashboard'))
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as w:
        pd.DataFrame({'Indicateur':['Total','Films','Séries','Ratings','Année min','Année max','Durée moy film (min)','Saisons moy série'],
                      'Valeur':[len(df),safe_int((df['type']=='Movie').sum()),safe_int((df['type']=='TV Show').sum()),
                                df['rating'].nunique(),safe_int(df['release_year'].min()),safe_int(df['release_year'].max()),
                                safe_float(df[df['type']=='Movie']['duration_minutes'].dropna().mean(),1),
                                safe_float(df[df['type']=='TV Show']['duration_seasons'].dropna().mean(),1)]
                     }).to_excel(w,index=False,sheet_name='KPIs')
        df['rating'].value_counts().reset_index().to_excel(w,index=False,sheet_name='Ratings')
        df['release_year'].value_counts().sort_index().reset_index().to_excel(w,index=False,sheet_name='Par année')
        df.head(2000)[['title','type','rating','release_year','duration_minutes','duration_seasons']].to_excel(w,index=False,sheet_name='Catalogue 2000')
    buf.seek(0)
    return send_file(buf,as_attachment=True,download_name=f'StayFlix_Netflix_{date.today()}.xlsx',
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

@app.route('/export/hotel')
@login_required
@roles_required('admin','manager')
def export_hotel():
    df = get_hotel()
    if df.empty: flash('Aucune donnée.','warning'); return redirect(url_for('hotel_dashboard'))
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as w:
        pd.DataFrame({'Indicateur':['Total','Annulations','Taux annulation (%)','ADR moyen','Revenu total','Types hotel','Pays'],
                      'Valeur':[len(df),safe_int(df['is_canceled'].sum()),safe_float(df['is_canceled'].mean()*100,1),
                                safe_float(df['adr'].mean()),safe_float(df['revenue'].sum(),0),df['hotel'].nunique(),df['country'].nunique()]
                     }).to_excel(w,index=False,sheet_name='KPIs')
        df.groupby('hotel').agg(Total=('is_canceled','count'),Annulations=('is_canceled','sum'),
                                 ADR=('adr','mean'),Revenu=('revenue','sum')).round(2).reset_index().to_excel(w,index=False,sheet_name='Par hotel')
        df.groupby('market_segment').agg(Total=('is_canceled','count'),Cancel_Rate=('is_canceled','mean'),
                                          ADR=('adr','mean')).round(2).reset_index().to_excel(w,index=False,sheet_name='Par segment')
        df.head(2000).to_excel(w,index=False,sheet_name='Données 2000')
    buf.seek(0)
    return send_file(buf,as_attachment=True,download_name=f'StayFlix_Hotel_{date.today()}.xlsx',
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

# ─── PROFIL / ADMIN ───────────────────────────────────────────────────────────
@app.route('/profile', methods=['GET','POST'])
@login_required
def profile():
    db = get_db()
    user = db.execute('SELECT * FROM users WHERE id=?',(session['user_id'],)).fetchone()
    if request.method == 'POST':
        fn=request.form.get('first_name','').strip(); ln=request.form.get('last_name','').strip()
        em=request.form.get('email','').strip(); ge=request.form.get('gender','')
        photo = user['photo']
        f = request.files.get('photo')
        if f and f.filename:
            ext = f.filename.rsplit('.',1)[-1].lower()
            if ext in ['jpg','jpeg','png','gif','webp']:
                fname = f"user_{session['user_id']}.{ext}"
                f.save(os.path.join(os.path.dirname(__file__),'static','uploads',fname)); photo = fname
        db.execute('UPDATE users SET first_name=?,last_name=?,email=?,gender=?,photo=? WHERE id=?',(fn,ln,em,ge,photo,session['user_id']))
        db.commit(); session.update({'first_name':fn,'last_name':ln,'photo':photo})
        flash('✅ Profil mis à jour !','success'); return redirect(url_for('profile'))
    return render_template('profile.html', user=user)

@app.route('/admin')
@login_required
@roles_required('admin','manager','analyst')
def admin_panel():
    db = get_db()
    users    = db.execute('SELECT * FROM users ORDER BY id').fetchall()
    contacts = db.execute('SELECT * FROM contacts ORDER BY created_at DESC LIMIT 50').fetchall()
    searches = db.execute('''SELECT sh.*,u.username FROM search_history sh LEFT JOIN users u ON sh.user_id=u.id ORDER BY sh.created_at DESC LIMIT 30''').fetchall()
    dn=get_netflix(); dh=get_hotel()
    ds = {'netflix':len(dn) if not dn.empty else 0, 'hotel':len(dh) if not dh.empty else 0,
          'users':len(users), 'contacts':len(contacts)}
    return render_template('admin.html', users=users, contacts=contacts, searches=searches, ds=ds)

@app.route('/admin/user/create', methods=['POST'])
@login_required
@roles_required('admin')
def admin_create_user():
    u=(request.form.get('username'),hp(request.form.get('password','')),
       request.form.get('first_name',''),request.form.get('last_name',''),
       request.form.get('email',''),request.form.get('gender','M'),request.form.get('role','viewer'))
    try:
        get_db().execute('INSERT INTO users (username,password,first_name,last_name,email,gender,role) VALUES (?,?,?,?,?,?,?)',u)
        get_db().commit(); flash(f"Utilisateur {u[0]} créé.",'success')
    except sqlite3.IntegrityError: flash("Nom d'utilisateur déjà pris.",'danger')
    return redirect(url_for('admin_panel'))

@app.route('/admin/user/<int:uid>/toggle', methods=['POST'])
@login_required
@roles_required('admin')
def admin_toggle(uid):
    if uid == session['user_id']: flash("Impossible de désactiver votre propre compte.",'danger'); return redirect(url_for('admin_panel'))
    db=get_db(); u=db.execute('SELECT is_active FROM users WHERE id=?',(uid,)).fetchone()
    nv=0 if u['is_active'] else 1
    db.execute('UPDATE users SET is_active=? WHERE id=?',(nv,uid)); db.commit()
    flash(f"Compte {'activé' if nv else 'désactivé'}.",'success'); return redirect(url_for('admin_panel'))

@app.route('/admin/user/<int:uid>/delete', methods=['POST'])
@login_required
@roles_required('admin')
def admin_delete(uid):
    if uid == session['user_id']: flash("Impossible de vous supprimer vous-même.",'danger'); return redirect(url_for('admin_panel'))
    get_db().execute('DELETE FROM users WHERE id=?',(uid,)); get_db().commit()
    flash('Utilisateur supprimé.','success'); return redirect(url_for('admin_panel'))

@app.route('/admin/contact/<int:cid>/read', methods=['POST'])
@login_required
def admin_read(cid):
    get_db().execute('UPDATE contacts SET is_read=1 WHERE id=?',(cid,)); get_db().commit()
    return redirect(url_for('admin_panel'))

@app.errorhandler(404)
def e404(e): return render_template('404.html'), 404
@app.errorhandler(500)
def e500(e): return render_template('404.html', error=True), 500

if __name__ == '__main__':
    init_db(); app.run(debug=True, host='0.0.0.0', port=5000)

# Expert DO !