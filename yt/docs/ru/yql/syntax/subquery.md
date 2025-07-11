
# Шаблоны подзапросов (subquery)

## DEFINE SUBQUERY {#define-subquery}

`DEFINE SUBQUERY` позволяет объявить шаблон подзапроса (subquery), который представляет собой параметризуемый блок из нескольких выражений верхнего уровня (statements), и затем многократно его использовать путём применения в секции `FROM` выражения [SELECT](select/index.md) или входных данных в [PROCESS](process.md)/[REDUCE](reduce.md) с указанием параметров.

В отличие от [действий](action.md) шаблон подзапроса должен заканчиваться выражением `SELECT`/`PROCESS`/`REDUCE`, чей результат и является возвращаемым значением подзапроса. При этом выражение верхнего уровня `SELECT`/`PROCESS`/`REDUCE` нельзя использовать более одного раза, как и модифицирующие выражения (например, `INSERT`).

После `DEFINE SUBQUERY` указывается:

1. [Именованное выражение](expressions.md#named-nodes), по которому объявляемый шаблон будет доступен далее в запросе.
2. В круглых скобках список именованных выражений, по которым внутри шаблона подзапроса можно обращаться к параметрам.
3. Ключевое слово `AS`.
4. Сам список выражений верхнего уровня.
5. `END DEFINE` выступает в роли маркера, обозначающего последнее выражение внутри шаблона подзапроса.

Один или более последних параметров subquery могут быть помечены вопросиком как необязательные — если они не были указаны при вызове subquery, то им будет присвоено значение `NULL`.

{% note info %}

В больших запросах объявление шаблонов подзапросов можно выносить в отдельные файлы и подключать их в основной запрос с помощью [EXPORT](export_import.md#export) + [IMPORT](export_import.md#import), чтобы вместо одного длинного текста получилось несколько логических частей, в которых проще ориентироваться. Важный нюанс: директива `USE my_cluster;` в импортирующем запросе не влияет на поведение объявленных в других файлах подзапросов.

{% endnote %}

Даже если список параметров в определении шаблона подзапроса пустой при использовании его во `FROM` секции нужно указать скобки `()`. Такое может быть удобно использовать, чтобы ограничить область видимости именованных выражений, используемых только в одном подзапросе.

В некоторых случаях вместо операции `DEFINE SUBQUERY` удобнее использовать эквивалентную форму в виде [лямбда функции](expressions.md#lambda).

В этом случае лямбда функция должна принимать первым аргументом специальный объект `world`, через который передаются зависимости о том, какие видны [PRAGMA](pragma/index.md) или [COMMIT](commit.md) в точке использования шаблона запроса. Также этот объект нужно передавать первым аргументом вместе с остальными аргументами при их наличии другим шаблонам запросов, если они используются в лямбда функции.

Возвращаемым значением лямбда функции должно быть значение с типом список структур (выходная таблица) или список вариантов над кортежом из структур (несколько выходных таблиц). В последнем случае в точке использования шаблона запроса обычно используется распаковка вида

```yql
$out1, $out2 = PROCESS $mySubquery($myParam1, $myParam2);
-- используем далее $out1 и $out2 как отдельные таблицы.
```

#### Примеры

```yql
DEFINE SUBQUERY $hello_world($name, $suffix?) AS
    $name = $name ?? ($suffix ?? "world");
    SELECT "Hello, " || $name || "!";
END DEFINE;

SELECT * FROM $hello_world(NULL); -- Hello, world!
SELECT * FROM $hello_world("John"); -- Hello, John!
SELECT * FROM $hello_world(NULL, "Earth"); -- Hello, Earth!
```

```yql
DEFINE SUBQUERY $dup($x) AS
   SELECT * FROM $x(1)  -- применяем переданный шаблон запроса с одним аргументом
   UNION ALL
   SELECT * FROM $x(2); -- ... и с другим аргументом
END DEFINE;

DEFINE SUBQUERY $sub($n) AS
   SELECT $n * 10;
END DEFINE;

SELECT * FROM $dup($sub); -- передаем шаблон запроса $sub как параметр
-- Результат:
-- 10
-- 20
```

```yql
/* Уберем используемые именованные выражения $a и $b в отдельную область видимости */
DEFINE SUBQUERY $clean() AS
   $a = 10;
   $b = $a * $a;
   SELECT $a AS a, $b AS b;
END DEFINE;

SELECT * FROM $clean(); -- a: 10, b: 100
```

```yql
USE hahn;

DEFINE SUBQUERY $input() as
    SELECT * FROM `home/yql/tutorial/users`;
END DEFINE;

DEFINE SUBQUERY $myProcess1($nestedQuery, $lambda) AS
    PROCESS $nestedQuery() -- использование скобок () тут обязательно
    USING $lambda(TableRow());
END DEFINE;

$myProcess2 = ($world, $nestedQuery, $lambda) -> {
    -- Если использовать ListFlatMap или YQL::OrderedFlatMap, то получится Ordered {{product-name}} Map операция
    return YQL::FlatMap($nestedQuery($world), $lambda);
};

-- При таком использовании реализации $myProcess1 и $myProcess2 идентичны
SELECT * FROM $myProcess1($input, ($x) -> { RETURN AsList($x, $x) });
SELECT * FROM $myProcess2($input, ($x) -> { RETURN AsList($x, $x) });
```

```yql
USE hahn;

DEFINE SUBQUERY $runPartition($table) AS
    $paritionByAge = ($row) -> {
        $recordType = TypeOf($row);
        $varType = VariantType(TupleType($recordType, $recordType));
        RETURN If($row.age % 2 == 0,
            Variant($row, "0", $varType),
            Variant($row, "1", $varType),
        );
    };

    PROCESS $table USING $paritionByAge(TableRow());
END DEFINE;

-- Распаковка двух результатов
$i, $j = (PROCESS $runPartition("home/yql/tutorial/users"));

SELECT * FROM $i;

SELECT * FROM $j;
```

## SubqueryExtend, SubqueryUnionAll, SubqueryMerge, SubqueryUnionMerge {#subquery-extend} {#subquery-unionall} {#subquery-merge} {#subquery-unionmerge}

Объединение шаблонов подзапросов `SubqueryExtend`, `SubqueryUnionAll`, `SubqueryMerge`, `SubqueryUnionMerge`.

Функции объединяют результаты одного и более шаблонов подзапросов, переданных аргументами. Требуется совпадение количества параметров в этих шаблонах подзапросов.

* `SubqueryExtend` требует совпадения схем подзапросов;
* `SubqueryUnionAll` работает по тем же правилам, что и [ListUnionAll](../builtins/list.md#ListUnionAll);
* `SubqueryMerge` использует те же ограничения, что и `SubqueryExtend`, а также выдаёт сортированный результат в случае если все подзапросы одинаково отсортированы;
* `SubqueryUnionMerge` использует те же ограничения, что и `SubqueryUnionAll`, а также выдаёт сортированный результат в случае если все подзапросы одинаково отсортированы.

#### Примеры

```yql
DEFINE SUBQUERY $sub1() as
    SELECT 1 as x;
END DEFINE;

DEFINE SUBQUERY $sub2() as
    SELECT 2 as x;
END DEFINE;

$s = SubqueryExtend($sub1,$sub2);
PROCESS $s();
```

## SubqueryExtendFor, SubqueryUnionAllFor, SubqueryMergeFor, SubqueryUnionMergeFor {#subquery-extend-for} {#subquery-unionall-for} {#subquery-merge-for} {#subquery-unionmerge-for}

Объединение шаблонов подзапросов после подстановки элемента списка `SubqueryExtendFor`, `SubqueryUnionAllFor`, `SubqueryMergeFor`, `SubqueryUnionMergeFor`.

Функции принимают аргументы:

* непустой список значений;
* шаблон подзапроса, в котором должен быть ровно один параметр.

И выполняют подстановку в шаблон подзапроса в качестве параметра каждый элемент из списка, после чего объединяют полученные подзапросы.

* `SubqueryExtendFor` требует совпадения схем подзапросов;
* `SubqueryUnionAllFor` работает по тем же правилам, что и [ListUnionAll](../builtins/list.md#ListUnionAll);
* `SubqueryMergeFor` использует те же ограничения, что и `SubqueryExtendFor`, а также выдаёт сортированный результат в случае, если все подзапросы одинаково отсортированы;
* `SubqueryUnionMergeFor` использует те же ограничения, что и `SubqueryUnionAllFor`, а также выдаёт сортированный результат в случае, если все подзапросы одинаково отсортированы.

#### Примеры

```yql
DEFINE SUBQUERY $sub($i) as
    SELECT $i as x;
END DEFINE;

$s = SubqueryExtendFor([1,2,3],$sub);
PROCESS $s();
```

## SubqueryOrderBy, SubqueryAssumeOrderBy

Добавление сортировки в шаблон подзапроса `SubqueryOrderBy` или указание о наличии таковой `SubqueryAssumeOrderBy`.

Данные функции принимают аргументы:

* шаблон подзапроса без параметров;
* список пар `(<string>, <boolean>)`, где:
   - первое значение &mdash; это имя колонки;
   - второе значение &mdash; направление сортировки. `true` для сортировки по возрастанию и `false` для сортировки по убыванию.

Функции выполняют построение нового шаблона запроса без параметров, в котором выполняется сортировка или добавляется указание о наличии сортировки к результату. Для использования полученного шаблона запроса необходимо использовать функцию `PROCESS`, так как при использовании `SELECT` сортировка будет проигнорирована.

#### Примеры

```yql
DEFINE SUBQUERY $sub() as
   SELECT * FROM (VALUES (1,'c'), (1,'a'), (3,'b')) AS a(x,y);
end define;

$sub2 = SubqueryOrderBy($sub, [('x',false), ('y',true)]);

PROCESS $sub2();
```


