import pytest
from httpx import ASGITransport, AsyncClient
from fluxmesh.api.app import app
from fluxmesh.core.models import JobPriority, JobStatus
from fluxmesh.storage.broker import FluxBroker


@pytest.mark.asyncio
async def test_api_workflow_endpoints(broker: FluxBroker):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Test Dashboard
        dash_res = await client.get("/dashboard")
        assert dash_res.status_code == 200
        assert "FluxMesh Cluster Dashboard" in dash_res.text

        # 2. Submit Job
        payload = {
            "task": "batch_process",
            "kwargs": {"items": ["a", "b", "c"]},
            "priority": 1,
            "idempotency_key": "api-test-key-1",
        }
        submit_res = await client.post("/api/v1/jobs", json=payload)
        assert submit_res.status_code == 201
        job_data = submit_res.json()
        job_id = job_data["id"]
        assert job_data["task"] == "batch_process"
        assert job_data["status"] == "PENDING"

        # 3. Query Job Status
        get_res = await client.get(f"/api/v1/jobs/{job_id}")
        assert get_res.status_code == 200
        assert get_res.json()["id"] == job_id

        # 4. Metrics
        metrics_res = await client.get("/api/v1/metrics")
        assert metrics_res.status_code == 200
        m = metrics_res.json()
        assert m["queue_depths"]["high"] >= 1

        # 5. Cancel Job
        cancel_res = await client.post(f"/api/v1/jobs/{job_id}/cancel")
        assert cancel_res.status_code == 200
        assert cancel_res.json()["status"] == "CANCELLED"

        # Verify status is updated
        updated_res = await client.get(f"/api/v1/jobs/{job_id}")
        assert updated_res.json()["status"] == "CANCELLED"

        # 6. Workers listing
        workers_res = await client.get("/api/v1/workers")
        assert workers_res.status_code == 200

        # 7. DLQ listing
        dlq_res = await client.get("/api/v1/dlq")
        assert dlq_res.status_code == 200
