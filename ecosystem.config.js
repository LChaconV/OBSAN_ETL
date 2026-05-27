const path = require('path');

module.exports = {
    apps: [
        {
            name: "api_beneficiarios_iraca",
            script: "./.venv/bin/python", 
            args: ["-m", "src.etl.api_beneficiarios_iraca.pipeline"], 
            cwd: __dirname,
            exec_mode: "fork",
            interpreter: "", 
            watch: false,
            autorestart: true,
            env: {
                PYTHONUNBUFFERED: "1"
            }
        },
        {
            name: "api_edu_escolar",
            script: "./.venv/bin/python", 
            args: ["-m", "src.etl.api_edu_escolar.pipeline"], 
            cwd: __dirname,
            exec_mode: "fork",
            interpreter: "", 
            watch: false,
            autorestart: true,
            env: {
                PYTHONUNBUFFERED: "1"
            }
        },
        {
            name: "api_edu_superior",
            script: "./.venv/bin/python", 
            args: ["-m", "src.etl.api_edu_superior.pipeline"], 
            cwd: __dirname,
            exec_mode: "fork",
            interpreter: "", 
            watch: false,
            autorestart: true,
            env: {
                PYTHONUNBUFFERED: "1"
            }
        },
        {
            name: "api_erradicacion_cultivos_coca",
            script: "./.venv/bin/python", 
            args: ["-m", "src.etl.api_erradicacion_cultivos_coca.pipeline"], 
            cwd: __dirname,
            exec_mode: "fork",
            interpreter: "", 
            watch: false,
            autorestart: true,
            env: {
                PYTHONUNBUFFERED: "1"
            }
        },
        {
            name: "api_familias_accion",
            script: "./.venv/bin/python", 
            args: ["-m", "src.etl.api_familias_accion.pipeline"], 
            cwd: __dirname,
            exec_mode: "fork",
            interpreter: "", 
            watch: false,
            autorestart: true,
            env: {
                PYTHONUNBUFFERED: "1"
            }
        },
        {
            name: "api_indice_riesgo_irca",
            script: "./.venv/bin/python", 
            args: ["-m", "src.etl.api_indice_riesgo_irca.pipeline"], 
            cwd: __dirname,
            exec_mode: "fork",
            interpreter: "", 
            watch: false,
            autorestart: true,
            env: {
                PYTHONUNBUFFERED: "1"
            }
        },
        {
            name: "api_minerales",
            script: "./.venv/bin/python", 
            args: ["-m", "src.etl.api_minerales.pipeline"], 
            cwd: __dirname,
            exec_mode: "fork",
            interpreter: "", 
            watch: false,
            autorestart: true,
            env: {
                PYTHONUNBUFFERED: "1"
            }
        },
        {
            name: "api_produc_gas",
            script: "./.venv/bin/python", 
            args: ["-m", "src.etl.api_produc_gas.pipeline"], 
            cwd: __dirname,
            exec_mode: "fork",
            interpreter: "", 
            watch: false,
            autorestart: true,
            env: {
                PYTHONUNBUFFERED: "1"
            }
        },
        {
            name: "api_produc_petroleo",
            script: "./.venv/bin/python", 
            args: ["-m", "src.etl.api_produc_petroleo.pipeline"], 
            cwd: __dirname,
            exec_mode: "fork",
            interpreter: "", 
            watch: false,
            autorestart: true,
            env: {
                PYTHONUNBUFFERED: "1"
            }
        },
        {
            name: "api_regalias",
            script: "./.venv/bin/python", 
            args: ["-m", "src.etl.api_regalias.pipeline"], 
            cwd: __dirname,
            exec_mode: "fork",
            interpreter: "", 
            watch: false,
            autorestart: true,
            env: {
                PYTHONUNBUFFERED: "1"
            }
        },
        {
            name: "api_victimas",
            script: "./.venv/bin/python", 
            args: ["-m", "src.etl.api_victimas.pipeline"], 
            cwd: __dirname,
            exec_mode: "fork",
            interpreter: "", 
            watch: false,
            autorestart: true,
            env: {
                PYTHONUNBUFFERED: "1"
            }
        }
    ]
};