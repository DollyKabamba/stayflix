from app import app, init_db
import sys

if __name__ == '__main__':
    init_db()
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
    print(f"\n🎬 StayFlix Analytics — http://localhost:{port}")
    print("   Admin: admin / AS3admin2026")
    print("   Manager: manager / Manager@2026")
    print("   Analyste: analyst / Analyst@2026")
    print("   Viewer: viewer / Viewer@2026\n")
    app.run(debug=False, host='0.0.0.0', port=port)
