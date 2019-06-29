from django.http import JsonResponse

def bounties(request):
    params = {}
    return JsonResponse(params, status=200, safe=False)
