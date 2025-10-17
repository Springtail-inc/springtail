def run_server(host, port, workers):
    import uvicorn
    uvicorn.run(
        "stpcl.imp.server.app:app",
        host=host,
        port=port,
        log_level="info",
        workers=workers,
        timeout_keep_alive=75,
        limit_concurrency=25,
    )
