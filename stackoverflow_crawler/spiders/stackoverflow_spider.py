import scrapy
import csv

class StackoverflowSpider(scrapy.Spider):
    name = "stackoverflow_spider"
    allowed_domains = ["stackoverflow.com"]
    start_urls = [
        'https://stackoverflow.com/questions/tagged/google-sheets'
    ]

    custom_settings = {
        'DOWNLOAD_DELAY': 2,  # Thêm độ trễ 2 giây giữa các yêu cầu
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 5,  # Tăng số lần retry lên 5
        'DOWNLOADER_MIDDLEWARES': {
            'stackoverflow_crawler.middlewares.TooManyRequestsRetryMiddleware': 550,
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,
        }
    }

    def parse(self, response):
        # Lấy danh sách các câu hỏi
        questions = response.css('div.s-post-summary')
        
        for question in questions:
            title = question.css('h3.s-post-summary--content-title a::text').get()
            question_url = question.css('h3.s-post-summary--content-title a::attr(href)').get()
            full_question_url = response.urljoin(question_url)
            excerpt = question.css('div.s-post-summary--content-excerpt::text').get().strip()
            answers = question.css('div.s-post-summary--stats-item.has-answers span.s-post-summary--stats-item-number::text').get()
            views = question.css('div.s-post-summary--stats-item span.s-post-summary--stats-item-number::text').re_first(r'\d+')
            author = question.css('div.s-user-card--info a::text').get()
            asked_time = question.css('time.s-user-card--time span::attr(title)').get()

            # Đảm bảo rằng nếu không có câu trả lời thì giá trị là 0
            if not answers:
                answers = "0"

            # Truyền thông tin của câu hỏi tới hàm parse_question
            yield scrapy.Request(
                url=full_question_url,
                callback=self.parse_question,
                meta={
                    'title': title,
                    'url': full_question_url,
                    'excerpt': excerpt,
                    'answers': answers,
                    'views': views,
                    'author': author,
                    'asked_time': asked_time
                }
            )
        
        # Xử lý phân trang
        next_page_url = response.css('a[rel="next"]::attr(href)').get()
        if next_page_url:
            yield scrapy.Request(response.urljoin(next_page_url), callback=self.parse)

    def parse_question(self, response):
        # Lấy dữ liệu từ meta
        title = response.meta['title']
        full_question_url = response.meta['url']
        excerpt = response.meta['excerpt']
        answers = response.meta['answers']
        views = response.meta['views']
        author = response.meta['author']
        asked_time = response.meta['asked_time']

        # Lấy nội dung câu hỏi đầy đủ (text)
        question_content = response.css('div.s-prose.js-post-body').get(default='').strip()

        # Lấy câu trả lời tốt nhất nếu có (text)
        best_answer = response.css('div.answer.accepted-answer div.s-prose.js-post-body').get(default='').strip()

        # Kiểm tra và thay thế giá trị trống bằng "chưa có câu trả lời"
        if not best_answer:
            best_answer = "chưa có câu trả lời"

        # Lưu vào CSV
        with open('stackoverflow_questions.csv', 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['title', 'url', 'excerpt', 'question_content', 'best_answer', 'answers', 'views', 'author', 'asked_time']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writerow({
                'title': title,
                'url': full_question_url,
                'excerpt': excerpt,
                'question_content': question_content,
                'best_answer': best_answer,
                'answers': answers,
                'views': views,
                'author': author,
                'asked_time': asked_time
            })

        # In dữ liệu ra để kiểm tra
        print({
            'title': title,
            'url': full_question_url,
            'excerpt': excerpt,
            'question_content': question_content,
            'best_answer': best_answer,
            'answers': answers,
            'views': views,
            'author': author,
            'asked_time': asked_time
        })
