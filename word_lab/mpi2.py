from mpi4py import MPI
from collections import Counter
import os
import glob
import time

DATASET_DIR = "/app/dataset"
CONSULTA_FILE = os.path.join(DATASET_DIR, "consulta.txt")

TAG_WORK = 1
TAG_RESULT = 2
TAG_FINISH = 3
TAG_SUMMARY = 4


def cargar_consulta(path_consulta):

    with open(path_consulta, "r", encoding="utf-8") as f:
        palabras = [line.strip().lower() for line in f if line.strip()]

    return set(palabras)


def procesar_archivo(path_archivo, consulta):

    contador = Counter()
    total_tokens = 0

    with open(path_archivo, "r", encoding="utf-8") as f:

        for linea in f:

            palabras = linea.lower().split()

            total_tokens += len(palabras)

            for palabra in palabras:

                if palabra in consulta:
                    contador[palabra] += 1

    return contador, total_tokens


def worker(comm, rank, consulta):

    contador_local = Counter()

    archivos_procesados = 0

    total_tokens = 0

    tiempo_inicio = time.time()

    while True:

        status = MPI.Status()

        archivo = comm.recv(
            source=0,
            tag=MPI.ANY_TAG,
            status=status
        )

        tag = status.Get_tag()

        if tag == TAG_FINISH:
            break

        contador_archivo, tokens_archivo = procesar_archivo(
            archivo,
            consulta
        )

        contador_local.update(contador_archivo)

        archivos_procesados += 1

        total_tokens += tokens_archivo

        comm.send(
            contador_archivo,
            dest=0,
            tag=TAG_RESULT
        )

    tiempo_local = time.time() - tiempo_inicio

    comm.send(
        (
            contador_local,
            archivos_procesados,
            total_tokens,
            tiempo_local
        ),
        dest=0,
        tag=TAG_SUMMARY
    )


def master(comm, size):

    consulta = cargar_consulta(CONSULTA_FILE)

    archivos = sorted(
        glob.glob(os.path.join(DATASET_DIR, "file_*.txt"))
    )

    total_archivos = len(archivos)

    consulta = comm.bcast(consulta, root=0)

    contador_global = Counter()

    siguiente_archivo = 0

    workers = size - 1

    tiempo_inicio = time.time()

    # Trabajo inicial
    for rank in range(1, size):

        if siguiente_archivo < total_archivos:

            comm.send(
                archivos[siguiente_archivo],
                dest=rank,
                tag=TAG_WORK
            )

            siguiente_archivo += 1

    workers_finalizados = 0

    while workers_finalizados < workers:

        status = MPI.Status()

        contador_parcial = comm.recv(
            source=MPI.ANY_SOURCE,
            tag=MPI.ANY_TAG,
            status=status
        )

        worker_rank = status.Get_source()

        tag = status.Get_tag()

        if tag == TAG_RESULT:

            contador_global.update(contador_parcial)

            if siguiente_archivo < total_archivos:

                comm.send(
                    archivos[siguiente_archivo],
                    dest=worker_rank,
                    tag=TAG_WORK
                )

                siguiente_archivo += 1

            else:

                comm.send(
                    None,
                    dest=worker_rank,
                    tag=TAG_FINISH
                )

                workers_finalizados += 1

    resumenes = []

    for _ in range(workers):

        resumen = comm.recv(
            source=MPI.ANY_SOURCE,
            tag=TAG_SUMMARY
        )

        resumenes.append(resumen)

    tiempo_total = time.time() - tiempo_inicio

    total_tokens = 0

    total_archivos_proc = 0

    print("\n===== RESULTADOS MPI v2 =====\n")

    print(f"Procesos utilizados: {size}")

    print(f"Archivos procesados: {total_archivos}")

    print("\nTiempos por proceso:")

    for i, resumen in enumerate(resumenes):

        _, archivos_proc, tokens_proc, tiempo_proc = resumen

        total_tokens += tokens_proc

        total_archivos_proc += archivos_proc

        print(
            f"Proceso {i+1}: "
            f"{archivos_proc} archivos | "
            f"{tiempo_proc:.6f} s"
        )

    print(f"\nTotal de tokens leídos: {total_tokens}")

    print(
        f"Total de ocurrencias encontradas: "
        f"{sum(contador_global.values())}"
    )

    print(f"\nTiempo total MPI v2: {tiempo_total:.6f} s")

    print("\nTop 10 palabras:")

    for palabra, freq in contador_global.most_common(10):

        print(f"  {palabra}: {freq}")


def main():

    comm = MPI.COMM_WORLD

    rank = comm.Get_rank()

    size = comm.Get_size()

    if rank == 0:

        master(comm, size)

    else:

        consulta = comm.bcast(None, root=0)

        worker(comm, rank, consulta)


if __name__ == "__main__":

    main()