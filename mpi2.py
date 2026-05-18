from mpi4py import MPI
from collections import Counter
import os
import glob
import time


TAG_REQUEST = 1
TAG_WORK = 2
TAG_STOP = 3
TAG_RESULT = 4

CHUNK_SIZE = 25


def cargar_consulta(consulta_path, case_sensitive=False):
    if not os.path.isfile(consulta_path):
        raise FileNotFoundError(
            f"No se encontro el archivo de consulta: {consulta_path}"
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


def serve_pending_requests(comm, queue, size, stopped_workers):
    """Atiende sin bloquear cualquier peticion pendiente de los workers."""
    status = MPI.Status()
    while comm.Iprobe(source=MPI.ANY_SOURCE, tag=TAG_REQUEST, status=status):
        worker = status.Get_source()
        comm.recv(source=worker, tag=TAG_REQUEST)
        if queue:
            chunk = queue[:CHUNK_SIZE]
            del queue[:CHUNK_SIZE]
            comm.send(chunk, dest=worker, tag=TAG_WORK)
        else:
            comm.send(None, dest=worker, tag=TAG_STOP)
            stopped_workers.add(worker)


def master(comm, size, dataset_dir, consulta_path, case_sensitive, top_n):
    palabras_objetivo = cargar_consulta(consulta_path, case_sensitive)
    comm.bcast(palabras_objetivo, root=0)

    files = sorted(glob.glob(os.path.join(dataset_dir, "file_*.txt")))
    total_archivos = len(files)
    queue = list(files)

    freq_global = Counter()
    tokens_por_rank = [0] * size
    archivos_por_rank = [0] * size
    tiempos_por_rank = [0.0] * size
    stopped_workers = set()

    t_total_start = MPI.Wtime()
    t0_local = MPI.Wtime()
    freq_local_master = Counter()

    while queue:
        serve_pending_requests(comm, queue, size, stopped_workers)
        if not queue:
            break
        chunk = queue[:CHUNK_SIZE]
        del queue[:CHUNK_SIZE]
        freq_chunk, tokens_chunk = contar_palabras_archivos(
            chunk, palabras_objetivo, case_sensitive
        )
        freq_local_master.update(freq_chunk)
        tokens_por_rank[0] += tokens_chunk
        archivos_por_rank[0] += len(chunk)

    tiempos_por_rank[0] = MPI.Wtime() - t0_local
    freq_global.update(freq_local_master)

    while len(stopped_workers) < size - 1:
        status = MPI.Status()
        comm.recv(source=MPI.ANY_SOURCE, tag=TAG_REQUEST, status=status)
        worker = status.Get_source()
        comm.send(None, dest=worker, tag=TAG_STOP)
        stopped_workers.add(worker)

    for _ in range(size - 1):
        status = MPI.Status()
        payload = comm.recv(source=MPI.ANY_SOURCE, tag=TAG_RESULT, status=status)
        worker = status.Get_source()
        freq_worker, tokens_worker, archivos_worker, tiempo_worker = payload
        freq_global.update(freq_worker)
        tokens_por_rank[worker] = tokens_worker
        archivos_por_rank[worker] = archivos_worker
        tiempos_por_rank[worker] = tiempo_worker

    t_total = MPI.Wtime() - t_total_start

    total_tokens = sum(tokens_por_rank)
    total_archivos_proc = sum(archivos_por_rank)
    total_ocurrencias = sum(freq_global.values())
    top_words = freq_global.most_common(top_n)
    t_max = max(tiempos_por_rank)
    t_min = min(tiempos_por_rank)
    t_avg = sum(tiempos_por_rank) / size
    imbalance = (t_max - t_min) / t_max if t_max > 0 else 0.0

    print("\n===== RESULTADOS MPI v2 (dynamic master-worker) =====\n")
    print(f"Procesos utilizados: {size}")
    print(f"Tamano de chunk: {CHUNK_SIZE} archivos")
    print(f"Archivos procesados: {total_archivos_proc} de {total_archivos}")
    print(f"Total de tokens leidos: {total_tokens}")
    print(f"Total de ocurrencias encontradas: {total_ocurrencias}")
    print("\nTiempos por proceso:")
    for i in range(size):
        rol = "(master)" if i == 0 else "(worker)"
        print(
            f"  Proceso {i} {rol}: "
            f"{archivos_por_rank[i]} archivos | "
            f"{tiempos_por_rank[i]:.6f} s"
        )
    print(
        f"\nLoad balance: t_min={t_min:.6f} s | "
        f"t_max={t_max:.6f} s | t_avg={t_avg:.6f} s | "
        f"imbalance={imbalance*100:.2f}%"
    )
    print(f"\nTiempo total MPI v2: {t_total:.6f} s")
    print(f"\nTop {top_n} palabras:")
    for palabra, cuenta in top_words:
        print(f"  {palabra}: {cuenta}")

    print(f"\nEXECUTION_TIME={t_total:.6f}")


def worker(comm, rank, case_sensitive):
    palabras_objetivo = comm.bcast(None, root=0)

    freq_local = Counter()
    total_tokens_local = 0
    archivos_locales = 0
    t0_local = MPI.Wtime()

    while True:
        comm.send(None, dest=0, tag=TAG_REQUEST)
        status = MPI.Status()
        chunk = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
        if status.Get_tag() == TAG_STOP:
            break
        freq_chunk, tokens_chunk = contar_palabras_archivos(
            chunk, palabras_objetivo, case_sensitive
        )
        freq_local.update(freq_chunk)
        total_tokens_local += tokens_chunk
        archivos_locales += len(chunk)

    tiempo_local = MPI.Wtime() - t0_local
    comm.send(
        (freq_local, total_tokens_local, archivos_locales, tiempo_local),
        dest=0,
        tag=TAG_RESULT,
    )


def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    dataset_dir = os.path.join(script_dir, "dataset")
    consulta_path = os.path.join(dataset_dir, "consulta.txt")
    case_sensitive = False
    top_n = 10

    if size < 2:
        if rank == 0:
            print("mpi2.py requiere al menos 2 procesos (1 master + 1 worker).")
            print("Para 1 proceso use baseline_secuencial.py.")
        return

    if rank == 0:
        master(comm, size, dataset_dir, consulta_path, case_sensitive, top_n)
    else:
        worker(comm, rank, case_sensitive)


if __name__ == "__main__":
    main()
