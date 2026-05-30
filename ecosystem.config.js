const pipelineSchedules = [
    { name: "api_edu_escolar", trigger: "cron", hour: 0, minute: 0 }, // Aprox 6 horas
    { name: "api_victimas", trigger: "cron", hour: 6, minute: 30 }, // Aprox 3 horas
    { name: "api_familias_accion", trigger: "cron", hour: 9, minute: 45 }, // Aprox 1 hora
    { name: "api_beneficiarios_iraca", trigger: "cron", hour: 11, minute: 0 }, // Aprox 2 minutos
    { name: "api_edu_superior", trigger: "cron", hour: 11, minute: 10 }, // Aprox 1 minuto
    { name: "api_erradicacion_cultivos_coca", trigger: "cron", hour: 11, minute: 15 }, // Aprox 1 minuto
    { name: "api_indice_riesgo_irca", trigger: "cron", hour: 11, minute: 20 }, // Aprox 1 minuto
    { name: "api_minerales", trigger: "cron", hour: 11, minute: 25 }, // Aprox 1 minuto
    { name: "api_produc_gas", trigger: "cron", hour: 11, minute: 30 }, // Aprox 1 minuto
    { name: "api_produc_petroleo", trigger: "cron", hour: 11, minute: 35 }, // Aprox 1 minuto
    { name: "api_regalias", trigger: "cron", hour: 11, minute: 40 }, // Aprox 1 minuto
];

module.exports = {
    apps: [
        {
            name: "etl_scheduler",
            script: "uv",
            args: ["run", "-m", "src.scheduler"],
            cwd: __dirname,
            exec_mode: "fork",
            interpreter: "none",
            watch: false,
            autorestart: true,
            restart_delay: 5000,
            max_memory_restart: "200M",
            env: {
                PYTHONUNBUFFERED: "1",
                ETL_TIMEZONE: "America/Bogota",
                ETL_MAX_CONCURRENT_JOBS: "1",
                ETL_JOB_MISFIRE_GRACE_TIME: "43200",
                ETL_SCHEDULES: JSON.stringify(pipelineSchedules),
            },
        },
    ],
};
