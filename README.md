# LORIN // SPINOFF — Production Status Dashboard

Dashboard de status de produção que lê o **ShotGrid (Flow Production Tracking)**
ao vivo e apresenta o andamento de um projeto de forma assertiva para
**produção e direção** — sem cara de planilha.

Feito para o projeto `1881_LORIN_SPINOFF` (SG project **354**), mas serve de
base para organizar qualquer projeto do SG.

## O que mostra

- **Hero** com colorscript do projeto (título diagramado).
- **Pontos em Aberto** — briefing de questões/decisões pendentes para a direção
  (CRUD persistido em SQLite).
- **Overview** — % concluído, contadores por bucket (done / em andamento / a fazer).
- **Por Status** — distribuição das tasks pelos status do SG.
- **Por Camada** — progresso por etapa do pipeline (Layout, Animation, Comp, …).
- **Cards por Shot / Asset** — thumbnail (puxado do SG) + status de cada step.
- **Comp Review** — preview do filme + vídeos de comp (Versions com task Comp).
- Botão **Refresh** re-puxa o SG ao vivo (nada de cron; pull sob demanda).

## Stack

- **Backend:** FastAPI + `requests` (cliente REST do SG, script auth, paginação).
- **Frontend:** HTML/CSS/JS single-page, identidade visual zombieStudio
  (Steelfish + Aeonik + JetBrains Mono, dark + rosa `#E87EAD`).
- **Persistência:** snapshot em disco (`snapshot.json`) + `questions.db` (SQLite).

## Rodar local

```bash
python -m venv .venv
.venv/Scripts/pip install -r requirements.txt   # (Scripts no Windows, bin no Linux)
# criar .env (ver abaixo)
.venv/Scripts/python -m uvicorn app:app --host 127.0.0.1 --port 3200
```

### `.env`

```
SHOTGRID_URL=https://<site>.shotgrid.autodesk.com
SHOTGRID_SCRIPT_NAME=<script>
SHOTGRID_SCRIPT_KEY=<key>
LORIN_PROJECT_ID=354
```

## Assets de runtime (fora do git)

`media/` guarda os binários grandes (preview do filme, `title.png` do hero) e é
ignorado pelo git — copie manualmente no deploy. As fontes da marca ficam em
`fonts/` (uso interno).

## Endpoints

| Método | Rota | Função |
|---|---|---|
| GET | `/` | dashboard |
| GET | `/api/data` | snapshot em cache |
| POST | `/api/refresh` | pull ao vivo do SG |
| GET | `/api/thumb/{type}/{id}` | thumbnail (Shot/Asset/Version) |
| GET | `/api/media/{vid}` | movie de uma Version (redirect S3) |
| GET/POST/PATCH/DELETE | `/api/questions` | pontos em aberto |
