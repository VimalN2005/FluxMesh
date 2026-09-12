import argparse
import asyncio
import json
import sys
import uvicorn
from rich.console import Console
from rich.table import Table

import fluxmesh.tasks  # Load builtins
from fluxmesh.core.models import JobPriority, JobSubmission
from fluxmesh.scheduler.scheduler import FluxScheduler
from fluxmesh.storage.broker import FluxBroker
from fluxmesh.worker.worker import FluxWorker

console = Console()


def run_server(args):
    console.print(f"[bold green]Starting FluxMesh API Server on {args.host}:{args.port}...[/bold green]")
    console.print(f"[bold cyan]Dashboard available at http://{args.host}:{args.port}/dashboard[/bold cyan]")
    uvicorn.run("fluxmesh.api.app:app", host=args.host, port=args.port, reload=args.reload)


def run_worker(args):
    console.print(f"[bold green]Starting FluxMesh Worker (concurrency={args.concurrency})...[/bold green]")
    queues = [q.strip() for q in args.queues.split(",")]
    worker = FluxWorker(concurrency=args.concurrency, queues=queues)
    try:
        asyncio.run(worker.run_until_interrupted())
    except KeyboardInterrupt:
        console.print("[yellow]Worker stopped gracefully.[/yellow]")


def run_scheduler(args):
    console.print(f"[bold green]Starting FluxMesh Scheduler (interval={args.interval}s)...[/bold green]")
    scheduler = FluxScheduler(tick_interval=args.interval)
    try:
        asyncio.run(scheduler.run_until_interrupted())
    except KeyboardInterrupt:
        console.print("[yellow]Scheduler stopped.[/yellow]")


async def _submit_job_async(args):
    broker = FluxBroker()
    await broker.connect()
    kwargs = {}
    if args.kwargs:
        try:
            kwargs = json.loads(args.kwargs)
        except Exception:
            console.print("[red]Invalid JSON in kwargs[/red]")
            return

    p = JobPriority.from_str(args.priority)
    sub = JobSubmission(task=args.task, kwargs=kwargs, priority=p, delay_seconds=args.delay)
    rec = await broker.submit_job(sub)
    console.print(f"[bold green]Job Submitted successfully![/bold green] ID: [cyan]{rec.id}[/cyan] (Status: {rec.status})")
    await broker.close()


def run_submit(args):
    asyncio.run(_submit_job_async(args))


def main():
    parser = argparse.ArgumentParser(prog="fluxmesh", description="FluxMesh Distributed Orchestration Platform")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Server
    p_server = subparsers.add_parser("server", help="Run FastAPI orchestration server")
    p_server.add_argument("--host", default="127.0.0.1", help="Bind host")
    p_server.add_argument("--port", type=int, default=8000, help="Bind port")
    p_server.add_argument("--reload", action="store_true", help="Enable reload")

    # Worker
    p_worker = subparsers.add_parser("worker", help="Start background worker process")
    p_worker.add_argument("--concurrency", type=int, default=4, help="Worker concurrency")
    p_worker.add_argument("--queues", default="critical,high,default,low", help="Comma-separated queues to consume")

    # Scheduler
    p_scheduler = subparsers.add_parser("scheduler", help="Start intelligent scheduler daemon")
    p_scheduler.add_argument("--interval", type=float, default=0.2, help="Scheduler tick interval in seconds")

    # Submit
    p_submit = subparsers.add_parser("submit", help="Submit a job to the cluster")
    p_submit.add_argument("--task", required=True, help="Task name")
    p_submit.add_argument("--kwargs", default="{}", help="Task payload kwargs JSON")
    p_submit.add_argument("--priority", default="default", help="Priority (critical, high, default, low)")
    p_submit.add_argument("--delay", type=float, default=None, help="Delay execution in seconds")

    args = parser.parse_args()
    if args.command == "server":
        run_server(args)
    elif args.command == "worker":
        run_worker(args)
    elif args.command == "scheduler":
        run_scheduler(args)
    elif args.command == "submit":
        run_submit(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
