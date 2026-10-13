"""Bounded resource inventory: no keys, URLs, response bodies or headers logged.

Targets include the repository's existing key-probe models as well as Lane's
current models. This does not change production routing or payment policy.
"""
import concurrent.futures,json,os,pathlib,re,time,urllib.request,urllib.error
from collections import defaultdict
from datetime import datetime,timezone
from src import lane_route as route

PROMPT='Reply exactly OK. No explanation.'
TARGETS=[
 ('gemini','GEMINI_API_KEY','gemini-3.5-flash-lite','gemini',None),
 ('gemini','GOOGLE_AI_STUDIO_API_KEY','gemini-3.5-flash-lite','gemini',None),
 ('gemini','GEMINI_API_KEY','gemini-2.5-flash','gemini',None),
 ('gemini','GOOGLE_AI_STUDIO_API_KEY','gemini-2.5-flash','gemini',None),
 ('gemini','GEMINI_API_KEY','gemini-2.5-pro','gemini',None),
 ('gemini','GOOGLE_AI_STUDIO_API_KEY','gemini-2.5-pro','gemini',None),
 ('groq','GROQ_API_KEY','openai/gpt-oss-120b','https://api.groq.com/openai/v1',None),
 ('groq','GROQ_API_KEY','openai/gpt-oss-20b','https://api.groq.com/openai/v1',None),
 ('sambanova','SAMBANOVA_API_KEY','Meta-Llama-3.3-70B-Instruct','https://api.sambanova.ai/v1',None),
 ('sambanova','SAMBANOVA_API_KEY','DeepSeek-V3.1','https://api.sambanova.ai/v1',None),
 ('zhipu','ZHIPU_API_KEY','glm-4.7-flash','https://open.bigmodel.cn/api/paas/v4',None),
 ('zhipu','GLM_API_KEY','glm-4.7-flash','https://open.bigmodel.cn/api/paas/v4',None),
 ('zhipu','ZHIPU_API_KEY','glm-5.3-flash','https://open.bigmodel.cn/api/paas/v4',None),
 ('zhipu','GLM_API_KEY','glm-5.3','https://api.z.ai/api/paas/v4',None),
 ('qwen','DASHSCOPE_API_KEY','qwen-flash','https://dashscope-intl.aliyuncs.com/compatible-mode/v1','DASHSCOPE_BASE_URL'),
 ('qwen','QWEN_API_KEY','qwen-flash','https://dashscope.aliyuncs.com/compatible-mode/v1',None),
 ('qwen','DASHSCOPE_API_KEY','qwen3-max','https://dashscope-intl.aliyuncs.com/compatible-mode/v1','DASHSCOPE_BASE_URL'),
 ('qwen','QWEN_API_KEY','qwen3-max','https://dashscope-intl.aliyuncs.com/compatible-mode/v1',None),
 ('mistral','MISTRAL_API_KEY','mistral-small-latest','https://api.mistral.ai/v1',None),
 ('mistral','MISTRAL_API_KEY','mistral-large-latest','https://api.mistral.ai/v1',None),
 ('nvidia_nim','NVIDIA_NIM_API_KEY','moonshotai/kimi-k2-instruct','https://integrate.api.nvidia.com/v1',None),
 ('nvidia_nim','NVIDIA_API_KEY','moonshotai/kimi-k2-instruct','https://integrate.api.nvidia.com/v1',None),
 ('nvidia_nim','NVIDIA_NIM_API_KEY','meta/llama-3.1-8b-instruct','https://integrate.api.nvidia.com/v1',None),
 ('siliconflow','SILICONFLOW_API_KEY','Qwen/Qwen3-8B','https://api.siliconflow.cn/v1',None),
 ('siliconflow','SILICONFLOW_API_KEY','moonshotai/Kimi-K2-Instruct','https://api.siliconflow.cn/v1',None),
 ('modelscope','MODELSCOPE_API_KEY','Qwen/Qwen3-8B','https://api-inference.modelscope.cn/v1',None),
 ('modelscope','MODELSCOPE_API_TOKEN','Qwen/Qwen3-8B','https://api-inference.modelscope.cn/v1',None),
 ('modelscope','MODELSCOPE_SDK_TOKEN','Qwen/Qwen3-8B','https://api-inference.modelscope.cn/v1',None),
 ('moonshot','MOONSHOT_API_KEY','moonshot-v1-8k','https://api.moonshot.cn/v1',None),
 ('openrouter','OPENROUTER_API_KEY','nvidia/nemotron-3-ultra-550b-a55b:free','https://openrouter.ai/api/v1',None),
 ('openrouter','OPENROUTER_API_KEY','google/gemma-4-31b-it:free','https://openrouter.ai/api/v1',None),
 ('openrouter','OPENROUTER_API_KEY','z-ai/glm-5.3','https://openrouter.ai/api/v1',None),
 ('tokenhub','TOKENHUB_API_KEY','glm-5.3-flash','https://tokenhub.tencentmaas.com/v1','TOKENHUB_BASE_URL'),
 ('tokenhub','TOKENHUB_API_KEY','glm-5.3','https://tokenhub.tencentmaas.com/v1','TOKENHUB_BASE_URL'),
 ('tokenhub','TENCENT_API_KEY','deepseek-v3.1','https://api.lkeap.cloud.tencent.com/v1','TENCENT_BASE_URL'),
 ('tokenhub','HUNYUAN_API_KEY','hunyuan-turbos-latest','https://api.hunyuan.cloud.tencent.com/v1',None),
 ('deepseek','DEEPSEEK_API_KEY','deepseek-chat','https://api.deepseek.com',None),
 ('pollinations','POLLINATIONS_API_KEY','deepseek','https://gen.pollinations.ai/v1',None),
 ('github_models','GITHUB_MODELS_TOKEN','openai/gpt-4o-mini','https://models.github.ai/inference',None),
 ('hf','HF_TOKEN','HuggingFaceTB/SmolLM3-3B','https://router.huggingface.co/v1',None),
 ('cloudflare','CLOUDFLARE_API_TOKEN','@cf/meta/llama-3.1-8b-instruct','cloudflare',None),
]

def status_kind(status,body):
    if status==200:return 'reachable'
    if status==401:return 'authentication_rejected'
    if status==402:return 'credit_or_billing_required'
    if status==403:return 'forbidden_or_account_restriction'
    if status==404:return 'endpoint_or_model_not_found'
    if status==429:return 'rate_or_quota_limit'
    if status>=500:return 'provider_service_error'
    if status==400:return 'request_or_model_configuration_rejected'
    return 'http_error'

def quota_scope(body):
    text=json.dumps(body).lower()
    if 'perday' in text or 'per_day' in text or 'per day' in text or 'daily' in text:return 'daily'
    if 'perminute' in text or 'per_minute' in text or 'per minute' in text:return 'minute'
    if 'perhour' in text or 'per_hour' in text or 'per hour' in text:return 'hour'
    if 'insufficient_quota' in text or 'exhausted' in text:return 'quota_exhausted_unspecified_window'
    return 'unspecified'

def probe(target):
    provider,key_name,model,base,base_env=target
    key=(os.environ.get(key_name) or '').strip();out={'provider':provider,'credential_name':key_name,'model_requested':model,'credential_configured':bool(key),'classification_route_enabled':provider in route.CLASSIFY_LANES,'model_on_current_classification_allowlist':model in route.primary_models_for(provider,'news_classify')}
    if not key:return out|{'status':'credential_missing','http_status':None,'visible_response':False}
    started=time.monotonic()
    if base=='gemini':
        url='https://generativelanguage.googleapis.com/v1beta/models/'+urllib.request.quote(model)+':generateContent?key='+urllib.request.quote(key)
        payload={'contents':[{'role':'user','parts':[{'text':PROMPT}]}],'generationConfig':{'maxOutputTokens':64,'temperature':0}}
        headers={'Content-Type':'application/json'}
    elif base=='cloudflare':
        account=(os.environ.get('CLOUDFLARE_ACCOUNT_ID') or '').strip()
        if not account:return out|{'status':'account_identifier_missing','http_status':None,'visible_response':False}
        url='https://api.cloudflare.com/client/v4/accounts/'+urllib.request.quote(account)+'/ai/v1/chat/completions'
        payload={'model':model,'messages':[{'role':'user','content':PROMPT}],'max_tokens':64}
        headers={'Content-Type':'application/json','Authorization':'Bearer '+key}
    else:
        endpoint=(os.environ.get(base_env) or base).rstrip('/') if base_env else base.rstrip('/')
        url=endpoint if endpoint.endswith('/chat/completions') else endpoint+'/chat/completions'
        payload={'model':model,'messages':[{'role':'user','content':PROMPT}],'max_tokens':64}
        headers={'Content-Type':'application/json','Authorization':'Bearer '+key}
    try:
        req=urllib.request.Request(url,json.dumps(payload).encode(),headers,method='POST')
        try:
            with urllib.request.urlopen(req,timeout=25) as response:status=response.status;body=json.loads(response.read());retry=response.headers.get('Retry-After','')
        except urllib.error.HTTPError as exc:
            status=exc.code;retry=exc.headers.get('Retry-After','')
            try:body=json.loads(exc.read())
            except Exception:body={}
        visible=''
        if status==200:
            if base=='gemini':visible=''.join(str(x.get('text') or '') for x in ((body.get('candidates') or [{}])[0].get('content') or {}).get('parts',[]) if not x.get('thought'))
            else:visible=str(((body.get('choices') or [{}])[0].get('message') or {}).get('content') or '')
        out.update(status=status_kind(status,body),http_status=status,visible_response=bool(visible.strip()),ok_response=bool(re.search(r'\bOK\b',visible,re.I)))
        if status==200 and not visible.strip():out['status']='reachable_no_visible_completion_at_probe_budget'
        if status==429:out['quota_scope']=quota_scope(body)
        if retry.isdigit():out['retry_after_seconds']=int(retry)
    except Exception as exc:out.update(status='transport_exception',exception_type=type(exc).__name__,http_status=None,visible_response=False)
    out['elapsed_seconds']=round(time.monotonic()-started,2)
    return out

def main():
    groups=defaultdict(list)
    for t in TARGETS:groups[t[0]].append(t)
    def group_work(items):
        rows=[]
        for t in items:
            rows.append(probe(t));time.sleep(.5)
        return rows
    result={'generated_at':datetime.now(timezone.utc).isoformat(),'purpose':'Tiny connectivity/completion probes of existing repo targets; not model accuracy tests or production routing changes','probe_output_token_cap':64,'results':[]}
    aliases=defaultdict(list)
    for name in sorted({t[1] for t in TARGETS}):
        key=(os.environ.get(name) or '').strip()
        if key:aliases[key].append(name)
    result['identical_credential_alias_groups']=[names for names in aliases.values() if len(names)>1]
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        for rows in executor.map(group_work,groups.values()):result['results'].extend(rows)
    path=pathlib.Path('resource_probe/results.json');path.parent.mkdir(exist_ok=True);path.write_text(json.dumps(result,indent=2))
    for row in result['results']:print(json.dumps(row),flush=True)
    # Only booleans for services needing environment-specific execution.
    print(json.dumps({'provider':'cloudflare','credential_configured':bool(os.environ.get('CLOUDFLARE_API_TOKEN')),'account_configured':bool(os.environ.get('CLOUDFLARE_ACCOUNT_ID')),'status':'inventory_only_requires_account_specific_request'}))
    print(json.dumps({'provider':'openclaw','endpoint_configured':bool(os.environ.get('OPENCLAW_GATEWAY_URL')),'credential_configured':bool(os.environ.get('OPENCLAW_TOKEN')),'status':'requires_ECS_local_gateway'}))

if __name__=='__main__':main()
