<?php
/**
 * Created by PhpStorm.
 * User: myslyvyi
 * Email: dima.myslyvyi@gmail.com
 * Date: 20.03.2018
 * Time: 15:15
 */

namespace Expenect;

use Exception;

/**
 * Class DB
 * @package Expenect
 */
class DB
{
    /** @var \mysqli mysqli */
    private $mysqli;

    /**
     * DB constructor.
     *
     * @param $host
     * @param $username
     * @param $password
     * @param $dbName
     *
     * @throws Exception
     */
    public function __construct($host, $username, $password, $dbName)
    {
        // Create new connect to DB
        $this->mysqli = new \mysqli(
            $host,
            $username,
            $password,
            $dbName
        );

        if ( ! mysqli_set_charset($this->mysqli, 'utf8')) {
            throw new \ErrorException(sprintf("Ошибка при загрузке набора символов utf8: %s\n",
                mysqli_error($this->mysqli)));
        }
    }

    /**
     * Mysqli query
     *
     * @param $sql
     *
     * @return bool|\mysqli_result
     */
    public function query($sql)
    {
        return $this->mysqli->query($sql);
    }

    /**
     * Get one row
     *
     * @param $sql
     *
     * @return bool|array
     */
    public function getRow($sql)
    {
        $query = $this->query($sql);
        $row = $query->fetch_row();

        return $row ? $row[0] : false;
    }

    /**
     * Get one record
     *
     * @param $sql
     * @param bool $isObject
     *
     * @return array|null|\stdClass
     */
    public function getRecord($sql, $isObject = false)
    {
        if ( ! $query = $this->query($sql)) {
            return $isObject ? null : [];
        }

        return $isObject ? $query->fetch_object() : $query->fetch_assoc();
    }

    /**
     * Fetch result
     *
     * @param $sql string
     *
     * @return mixed
     */
    public function fetchAll($sql)
    {
        $query = $this->query($sql);

        return $query->fetch_all(MYSQLI_ASSOC);
    }

    /**
     * Екранирование символов
     *
     * @param $value
     *
     * @return mixed
     */
    public function pSql($value)
    {
        $search = ["\\", "\0", "\n", "\r", "\x1a", "'", '"'];
        $replace = ["\\\\", "\\0", "\\n", "\\r", "\Z", "\'", '\"'];

        return str_replace($search, $replace, $value);
    }

    /**
     * Обновим значения глобальным запросом
     *
     * @param $tableName
     * @param array $data
     * @param array $keys
     *
     * @return string
     */
    public function sqlBulkUpdate ($tableName, $data, $keys)
    {
        $fields = [];
        $cases = [];
        $where = [];

        foreach ($data as $i => $row) {
            $cases[$i] = '';
            $row = array_map([$this, 'pSql'], $row);

            foreach ($row as $fld => $val) {
                if (in_array($fld, $keys)) {
                    $cases[$i] .= "{$fld} = '{$val}' AND ";

                    $where[$fld][] = $val;
                } else {
                    $fields[$fld][$i] = $val;
                }
            }

            $cases[$i] = rtrim($cases[$i], 'AND ');
        }

        // --
        $sql = 'UPDATE ' . $tableName . ' SET ';

        foreach ($fields as $fld => $vals) {
            $sql .= $fld . ' = CASE';

            foreach ($vals as $i => $val) {
                $sval = is_null($val) ? 'NULL' : '\'' . $val . '\'';
                $sql .= ' WHEN ' . $cases[$i] . ' THEN ' . $sval;
            }

            $sql .= ' END, ';
        }

        $sql = rtrim($sql, ', ');
        $sql .= ' WHERE ';

        foreach ($where as $fld => $vals) {
            $sql .= $fld . ' IN (\'' . implode('\',\'', array_unique($vals)) . '\') AND ';
        }

        return rtrim($sql, ' AND ').';';
    }

    public function _escape($string)
    {
        return mysqli_real_escape_string($this->mysqli, $string);
    }

    /**
     * Сформирируем массовый insert и добавим в файл
     *
     * @param $tableName
     * @param $data
     * @param $fileName
     *
     * @return bool|int
     */
    public function sqlBulkInsertAppendFile($tableName, $data, $fileName = null)
    {

        $sql = $this->sqlBulkInsert($tableName, $data);

        return file_put_contents('sql/' . $fileName . '.sql', $sql, FILE_APPEND);
    }

    /**
     * Сформирируем массовый insert и запишем в файл (если файл существует он будет перезаписан)
     *
     * @param $tableName
     * @param $data
     * @param null $fileName
     *
     * @return bool|int
     */
    public function sqlBulkInsertFile($tableName, $data, $fileName = null)
    {
        $sql = $this->sqlBulkInsert($tableName, $data);

        return file_put_contents('sql/' . ($fileName ?? $tableName) . '.sql', $sql, 0);
    }

    /**
     * Массовый insert который после формирования будет выполнен
     *
     * @param $tableName
     * @param $data
     *
     * @return bool|\mysqli_result
     */
    public function sqlBulkInsertExec($tableName, $data)
    {
        $sql = $this->sqlBulkInsert($tableName, $data);

        return $this->mysqli->query($sql);
    }

    /**
     * Сформирируем массовый sql запрос на добавление данных
     *
     * @param $tableName
     * @param $data array в качестве ключа должны виступать поля в БД
     *
     * @return bool|string
     */
    public function sqlBulkInsert($tableName, $data)
    {
        if (\count($data) === 0 || \count($data[0]) === 0) {
            return false;
        }

        // Получим наши ключи
        $keys = array_keys($data[0]);
        // Наш основной запрос
        $sql = /** @lang text */
            'INSERT INTO ' . $tableName . ' (`' . implode('`,`', $keys) . '`) VALUES ';

        /** @var array $item */
        foreach ($data as $item) {
            // pSql
            foreach ($item as $key => $it) {
                if ($it === null) {
                    $item[$key] = 'NULL';
                } elseif (\is_string($it)) {
                    $item[$key] = '\'' . $this->pSql($it) . '\'';
                } else {
                    /** @var array $it */
                    $item[$key] = $it;
                }
            }

            $sql .= '(' . implode(',', $item) . '),';
        }
        $sql = substr($sql, 0, -1) . ';' . PHP_EOL;


        return $sql;
    }
}
