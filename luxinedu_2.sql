CREATE TABLE luxinedu_2.groups
    (
    `group_id` UInt8,
    `ta_id` UInt8,
    `student_id` UInt8,
    `book_id` String,
    `group_name` String
    )
ENGINE = MergeTree()
PRIMARY KEY (group_id);

CREATE TABLE luxinedu_2.teaching_advisors
    (
    `ta_id` UInt8,
    `ta_name` String
    )
ENGINE = MergeTree()
PRIMARY KEY (ta_id);

CREATE TABLE luxinedu_2.attendance
(
date Date,
student_id UInt8,
book_unit_lesson String,
book_unit_lesson_2 String default Null,
homework UInt8 default Null,
behaviour UInt8 default Null,
comprehension UInt8 default Null,
vocabulary UInt8 default Null,
speaking UInt8 default Null,
reading UInt8 default Null,
writing UInt8 default Null
)
ENGINE = MergeTree()
ORDER BY (date);

CREATE TABLE luxinedu_2.books
    (
    `book_id` String,
    `book_name` String
    )
    ENGINE = MergeTree()
    PRIMARY KEY (book_id);

CREATE TABLE luxinedu_2.students
    (
    `student_id` UInt8,
    `ta_id` UInt8,
    `student_name` String
    )
    ENGINE = MergeTree()
    PRIMARY KEY (student_id);