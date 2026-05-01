"""
GET    /api/v2/views        — list all saved views
POST   /api/v2/views        — create a view
DELETE /api/v2/views/{name} — delete a view
"""

from __future__ import annotations

import json
from typing import Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from ochain_v2.api.deps import MetaDep

router = APIRouter()


class ViewBody(BaseModel):
    name: str
    query: dict


@router.get("/api/v2/views")
async def list_views(meta: MetaDep):
    views = meta.get_views()
    return JSONResponse({"views": views})


@router.post("/api/v2/views", status_code=201)
async def create_view(body: ViewBody, meta: MetaDep):
    view_id = meta.save_view(body.name, body.query)
    return JSONResponse({"id": view_id, "name": body.name}, status_code=201)


@router.delete("/api/v2/views/{name}")
async def delete_view(name: str, meta: MetaDep):
    deleted = meta.delete_view(name)
    if not deleted:
        raise HTTPException(404, f"View {name!r} not found")
    return JSONResponse({"ok": True})
