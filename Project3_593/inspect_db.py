import sqlite3

for db in ("pencilpros.db", "paypal.db", "pencilpros_prov.db", "paypal_prov.db"):
    print("\n==", db, "==")
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row

    if db.endswith("_prov.db"):
        rows = conn.execute(
            "SELECT op,src_app,dst_app,user_id,tag_id,t_unix,meta "
            "FROM provenance ORDER BY t_unix"
        ).fetchall()
        for r in rows:
            print(" ", dict(r))

    elif db == "pencilpros.db":
        print("users:")
        for r in conn.execute("SELECT * FROM users"):
            print(" ", dict(r))
        print("purchases:")
        for r in conn.execute("SELECT * FROM purchases"):
            print(" ", dict(r))

    else:  # paypal.db
        for r in conn.execute("SELECT * FROM payments"):
            print(" ", dict(r))

    conn.close()
