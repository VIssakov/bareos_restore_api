# bareos_restore_api

## Описание

Восстановление бэкапов через bareos bvfs api.

Для работы скрипта нужны следующие пакеты:
- python-bareos (ставится из репозитория bareos)
- sslpsk (ставится через pip)

------------------

## Пример использования
```bash
python3 bareos_restore_api.py --restore --sclient <client_name> /source/for/restore/ --dclient <client_name> --dpath /
```

где:
--sclient - имя клиента с которого будем восстанавливать данные
--sdata - что будем восстанавливать
--dclient - имя клиента куда будем восстанавливать
--dpath - в какую директорию будем восстанавливать

------------------
***Автор:*** Исаков Валерий <isakovvv@yamoney.ru>