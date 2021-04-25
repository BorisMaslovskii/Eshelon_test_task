Написать простой парсер WHERE части запросов SQL.

Парсер должен:
- иметь сигнатуру Parse(query string, qb squirrel.SelectBuilder)(*squirrel.SelectBuilder, error)
- уметь разбирать пользовательский ввод вида Field1 = "foo" AND Field2 != 7 OR Field3 > 11.7
- поддерживать синтаксис PostgreSQL;
- поддерживать точки в имени колонки - Foo.Bar.Alpha;
- вызывать callback-обработчик для проверки валидности имени колонки и соответствия типов колонки и значения, возможная сигнатура обработчика - func(colName string, value interface{}) error
- обрабатывать только блок условий WHERE, то есть возвращать в ошибку в случае если во входной строке есть другие выражения SQL, как то LIMIT, ORDER BY и др.
- в случае успеха разбора и проверки входных условий - формировать WHERE условия для qb squirrel.SelectBuilder вида qb =
qb.Where(squirrel.Eq{left: val})
- возвращать qb дополненный выражениями извлечёнными из пользовательского ввода query

При желании можно решить задание повышенной сложности - пользовательский ввод разбирать парсером SQL от CockroachDB. Пример работы с парсером можно посмотреть в https://github.com/cockroachdb/cockroach-gen/blob/master/pkg/sql/parser/parse_test.go

Для импорта парсера как зависимости потребуется сделать форк исходного репозитория и значимо урезать содержимое репозитория, оставив лишь необходимые для работы парсера части кода.

Тестовые примеры для работы парсера:

- Foo.Bar.X = 'hello'
- Bar.Alpha = 7
- Foo.Bar.Beta > 21 AND Alpha.Bar != 'hello'
- Alice.IsActive AND Bob.LastHash = 'ab5534b'
- Alice.Name ~ 'A.*` OR Bob.LastName !~ 'Bill.*`

Выполненное тестовое задание разместите на github. Ссылку прошу направить мне обратным сообщением или на почту yn@npo-echelon.ru.