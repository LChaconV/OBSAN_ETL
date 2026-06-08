const pipelineSchedules = [
    { name: "api_beneficiarios_iraca", trigger: "cron", hour: 0, minute: 0 }, // Aprox 2 minutos
    { name: "api_edu_superior", trigger: "cron", hour: 0, minute: 10 }, // Aprox 1 minuto
    { name: "api_erradicacion_cultivos_coca", trigger: "cron", hour: 0, minute: 15 }, // Aprox 1 minuto
    { name: "api_indice_riesgo_irca", trigger: "cron", hour: 0, minute: 20 }, // Aprox 1 minuto
    { name: "api_minerales", trigger: "cron", hour: 0, minute: 25 }, // Aprox 1 minuto
    { name: "api_produc_gas", trigger: "cron", hour: 0, minute: 30 }, // Aprox 1 minuto
    { name: "api_produc_petroleo", trigger: "cron", hour: 0, minute: 35 }, // Aprox 1 minuto
    { name: "api_regalias", trigger: "cron", hour: 0, minute: 40 }, // Aprox 1 minuto
    { name: "url_terraclimate", trigger: "cron", hour: 0, minute: 45}, // Aprox 1 hora
    { name: "api_edu_escolar", trigger: "cron", hour: 1, minute: 45 }, // Aprox 2 horas
    { name: "api_victimas", trigger: "cron", hour: 3, minute: 30 }, // Aprox 3 hora
    { name: "api_familias_accion", trigger: "cron", hour: 6, minute: 30 }, // Aprox 1 hora
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
            max_memory_restart: "00M",
            env: {
                PYTHONUNBUFFERED: "1",
                PYTHONIOENCODING: "utf-8",
                ETL_TIMEZONE: "America/Bogota",
                ETL_MAX_CONCURRENT_JOBS: "1",
                ETL_JOB_MISFIRE_GRACE_TIME: "4300",
                ETL_SCHEDULES: JSON.stringify(pipelineSchedules),
            },
        },
    ],
};
