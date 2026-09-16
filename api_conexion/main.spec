# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['C:/Users/USUARIO/Documents/API PARA CAPTADOR/api_conexion/main.py'],
    pathex=[],
    binaries=[],
    datas=[('C:/Users/USUARIO/Documents/API PARA CAPTADOR/config.txt', '.'), ('C:/Users/USUARIO/Documents/API PARA CAPTADOR/api_conexion/msodbcsql18_x64.msi', '.')],
    hiddenimports=['pyodbc', 'sqlalchemy.dialects.mssql', 'sqlalchemy.dialects.mssql.pyodbc', 'uvicorn.logging', 'uvicorn.loops', 'uvicorn.loops.auto', 'uvicorn.protocols', 'uvicorn.protocols.http', 'uvicorn.protocols.http.auto', 'uvicorn.lifespan', 'uvicorn.lifespan.on'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='main',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='main',
)
