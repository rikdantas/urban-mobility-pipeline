import subprocess
import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


PIPELINE_STEPS = [
    ("Bronze - Extract", "src/bronze/bronze_extract.py"),
    ("Silver - Tempo Real Ônibus", "src/silver/silver_tempo_real_onibus.py"),
    ("Silver - MCO", "src/silver/silver_mco.py"),
    ("Gold - Ônibus Heatmap", "src/gold/gold_onibus_heatmap.py"),
    ("Gold - Ônibus Ativos por Linha/Dia", "src/gold/gold_onibus_ativos_por_linha_dia.py"),
    ("Gold - MCO Demanda Linha/Dia/Hora", "src/gold/gold_mco_demanda_linha_dia_hora.py"),
    ("Gold - MCO Top Linhas", "src/gold/gold_mco_top_linhas.py"),
]


def run_step(name: str, script_path: str):
    print(f"\n🚀 Iniciando etapa: {name}")
    start = time.time()

    full_path = PROJECT_ROOT / script_path

    result = subprocess.run(
        [sys.executable, str(full_path)],
        capture_output=False,
    )

    if result.returncode != 0:
        print(f"\n❌ Erro na etapa: {name}")
        sys.exit(1)

    duration = time.time() - start
    print(f"✅ Etapa concluída: {name} ({duration:.2f}s)")


def main():
    print("=" * 60)
    print("🚀 EXECUTANDO PIPELINE COMPLETO - URBAN MOBILITY")
    print("=" * 60)

    total_start = time.time()

    for name, script in PIPELINE_STEPS:
        run_step(name, script)

    total_duration = time.time() - total_start

    print("\n" + "=" * 60)
    print("🎉 PIPELINE EXECUTADO COM SUCESSO")
    print(f"⏱ Tempo total: {total_duration:.2f} segundos")
    print("=" * 60)


if __name__ == "__main__":
    main()
