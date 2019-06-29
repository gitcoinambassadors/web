from django.urls import path

from checkin import views

app_name = 'checkin'
urlpatterns = [
    path('bounties/', views.bounties, name='bounties')
]
