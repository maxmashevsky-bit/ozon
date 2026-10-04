TRUNCATE comments,posts RESTART IDENTITY;
INSERT INTO posts(author_id,title,text) SELECT 'bench','Post '||g,repeat('x',200) FROM generate_series(1,100) g;
INSERT INTO comments(post_id,author_id,text) SELECT p,'bench',repeat('x',100) FROM generate_series(1,100) p CROSS JOIN generate_series(1,100) r ORDER BY p,r;
INSERT INTO comments(post_id,parent_id,author_id,text) SELECT post_id,id,'bench',repeat('y',100) FROM comments CROSS JOIN generate_series(1,4) g WHERE parent_id IS NULL ORDER BY id,g;
ANALYZE posts;
ANALYZE comments;
