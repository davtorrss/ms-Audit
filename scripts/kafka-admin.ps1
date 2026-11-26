param(
    [ValidateSet("listar", "describir", "crear")]
    [string]$Accion = "listar"
)

$contenedor = "vetcare-kafka-local"
$servidor = "localhost:29092"

$topicos = @(
    @{
        Nombre = "consultations.events"
        Retencion = "604800000"
        Politica = "delete"
    },
    @{
        Nombre = "audit.timeline"
        Retencion = "2592000000"
        Politica = "compact,delete"
    },
    @{
        Nombre = "consultations.events.audit.DLT"
        Retencion = "1209600000"
        Politica = "delete"
    }
)

function Ejecutar-Kafka {
    param([string[]]$Argumentos)

    & docker exec $contenedor @Argumentos

    if ($LASTEXITCODE -ne 0) {
        throw "El comando Kafka fallo."
    }
}

switch ($Accion) {

    "listar" {
        Write-Host "Topicos disponibles en Kafka:"

        Ejecutar-Kafka -Argumentos @(
            "kafka-topics",
            "--bootstrap-server", $servidor,
            "--list"
        )
    }

    "describir" {
        foreach ($topico in $topicos) {

            Write-Host "`n=== $($topico.Nombre) ==="

            Ejecutar-Kafka -Argumentos @(
                "kafka-topics",
                "--bootstrap-server", $servidor,
                "--describe",
                "--topic", $topico.Nombre
            )

            Ejecutar-Kafka -Argumentos @(
                "kafka-configs",
                "--bootstrap-server", $servidor,
                "--entity-type", "topics",
                "--entity-name", $topico.Nombre,
                "--describe"
            )
        }
    }

    "crear" {
        foreach ($topico in $topicos) {

            Write-Host "Verificando topico: $($topico.Nombre)"

            Ejecutar-Kafka -Argumentos @(
                "kafka-topics",
                "--bootstrap-server", $servidor,
                "--create",
                "--if-not-exists",
                "--topic", $topico.Nombre,
                "--partitions", "3",
                "--replication-factor", "1",
                "--config", "retention.ms=$($topico.Retencion)",
                "--config", "cleanup.policy=$($topico.Politica)"
            )
        }
    }
}
