from mpi4py import MPI
from collections import Counter
import os
import glob
import time

def cargar_consulta(consulta_path, case_sensitive=False):

    if not os.path.isfile(consulta_path):
        raise FileNotFoundError(
            f"No se encontró el archivo de consulta: {consulta_path}"
        )

    with open(consulta_path, "r", encoding="utf-8") as f:
        palabras = [line.strip() for line in f if line.strip()]

    if not case_sensitive:
        palabras = [w.lower() for w in palabras]

    return set(palabras)

def contar_palabras_archivos(files, palabras_objetivo, case_sensitive=False):

    freq_local = Counter()
    total_tokens = 0

    for ruta in files:

        with open(ruta, "r", encoding="utf-8") as f:

            for linea in f:

                palabras = linea.split()

                if not case_sensitive:
                    palabras = [w.lower() for w in palabras]

                total_tokens += len(palabras)

                for w in palabras:
                    if w in palabras_objetivo:
                        freq_local[w] += 1

    return freq_local, total_tokens

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

script_dir = os.path.dirname(os.path.abspath(__file__))
dataset_dir = os.path.join(script_dir, "dataset")

consulta_name = "consulta.txt"
case_sensitive = False
top_n = 10

if rank == 0:

    consulta_path = os.path.join(dataset_dir, consulta_name)

    palabras_objetivo = cargar_consulta(
        consulta_path,
        case_sensitive=case_sensitive
    )

    files = sorted(
        glob.glob(os.path.join(dataset_dir, "file_*.txt"))
    )

else:

    palabras_objetivo = None
    files = None
    
palabras_objetivo = comm.bcast(palabras_objetivo, root=0)

if rank == 0:
    chunks = [files[i::size] for i in range(size)]
else:
    chunks = None

local_files = comm.scatter(chunks, root=0)

print(f"Proceso {rank}: {len(local_files)} archivos asignados")

t0_local = time.perf_counter()

freq_local, total_tokens_local = contar_palabras_archivos(
    local_files,
    palabras_objetivo,
    case_sensitive=case_sensitive
)

t1_local = time.perf_counter()

local_time = t1_local - t0_local

all_freqs = comm.gather(freq_local, root=0)

all_tokens = comm.gather(total_tokens_local, root=0)

all_times = comm.gather(local_time, root=0)

all_nfiles = comm.gather(len(local_files), root=0)

if rank == 0:

    freq_global = Counter()

    for freq in all_freqs:
        freq_global.update(freq)

    total_tokens = sum(all_tokens)

    total_ocurrencias = sum(freq_global.values())

    top_words = freq_global.most_common(top_n)

    print("\n===== RESULTADOS MPI v1 =====\n")

    print(f"Procesos utilizados: {size}")
    print(f"Archivos procesados: {sum(all_nfiles)}")
    print(f"Total de tokens leídos: {total_tokens}")
    print(f"Total de ocurrencias encontradas: {total_ocurrencias}")

    print("\nTiempos por proceso:")

    for i in range(size):
        print(
            f"Proceso {i}: "
            f"{all_nfiles[i]} archivos | "
            f"{all_times[i]:.6f} s"
        )

    print(f"\nTop {top_n} palabras:")

    for palabra, cuenta in top_words:
        print(f"  {palabra}: {cuenta}")