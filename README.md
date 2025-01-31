# Задание на практическую работу по модулю Kafka

### Задание #1 (Производители Kafka)
- Необходимо реализовать модуль для загрузки объектов в Kafka;
- Конфигурирование производителя должно быть вынесено в property-файл;
- В качестве данных необходимо сформировать объекты, описывающие транзакцию (тип операции/сумма/счёт/дата);
- Формат передачи данных JSON;
- Перед отправкой сообщения должны валидироваться по схеме json-schema;
- Разные типы операций должны записываться в разные партиции топика;
- Продюсер должен быть максимально производительным и гарантировать доставку всех транзакций брокеру;
- В случае сбоя продюсер должен фиксировать в логе ошибку, смещение и партицию битого сообщения;
- Код должен быть задокументирован;
- Логируем все значимые кейсы;

### Задание #2 (Потребители Kafka)
- Необходимо реализовать модуль для выгрузки объектов из Kafka;
- Конфигурирование потребителя должно быть вынесено в property-файл;
- В качестве данных необходимо сформировать объекты, описывающие транзакцию (тип операции/сумма/счёт/дата);
- Формат передачи данных JSON;
- Перед обработкой сообщения должны валидироваться по схеме json-schema;
- Потребитель должен быть максимально производительным и гарантировать обработку всех транзакций брокера;
- В случае сбоя продюсер потребитель должен фиксировать текущий offset и при перезапуске начинать читать с него же;
- Код должен быть задокументирован;
- Логируем все значимые кейсы;


### Задание #3 (Внутреннее устройство Kafka)
- Необходимо доработать производитель Kafka (из задания 1) для возможности получения подтверждения об успешной обработке событий через топик обратного потока;
- Вариант подтверждения - подсчитывать контрольную сумму по идентификаторам сообщений на основе временных срезов;
- В случае, если разойдутся контрольные суммы, необходимо повторить отправку всех битых сообщений;
- Код должен быть задокументирован;
- Логируем все значимые кейсы;

### Ограничения
- Не используем Spring или другие верхнеуровневые библиотеки.

### Как работать над заданием?
- Создаёте от main свою новую ветку с типом release и с именем по шаблону "Фамилия_Модуль_Задание" (например: release/taranov_kafka_1);
- Клонируете проект из новой ветки в локальный репозиторий;
- Делаете доработки у себя в локальном репозитории;
- Заливаете свои доработки в новую feature ветку (например feature/taranov_kafka_1);
- Делаете PR в свою исходную релизную ветку (из которой клонировали проект);

### Условие успешной сдачи работы (критерии приемки)
- Проект компилируется и запускается;
- Контекст Spring, алгоритм выполнения и функциональность проекта должны остаться неизменны;
- Результат review PR = approve.

### Запуск

[Команды запуска и настройки Kafka](kafka-commands.md)
