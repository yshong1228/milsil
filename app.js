
var GAMES = typeof _GAMES_DATA !== 'undefined' ? _GAMES_DATA : [];
var openRecs = new Set();
var IS_FILE_PROTOCOL = window.location.protocol === 'file:';
var FIRESTORE_POLL_MS = IS_FILE_PROTOCOL ? 6000 : 12000;
var firestoreBindings = {};
var LOCAL_DB_PREFIX = 'milsil_localdb_v2_';
var __memoryLocalStore = {};
var USING_LOCAL_DB = false;
var liveSyncEnabled = false;
var db = null;

function deepClone(value){
  if(value===undefined)return undefined;
  return JSON.parse(JSON.stringify(value));
}
function safeStoreGet(key){
  try{return window.localStorage.getItem(key);}catch(e){}
  return Object.prototype.hasOwnProperty.call(__memoryLocalStore,key)?__memoryLocalStore[key]:null;
}
function safeStoreSet(key,value){
  try{
    window.localStorage.setItem(key,value);
    return;
  }catch(e){}
  __memoryLocalStore[key]=value;
}
function safeSessionGet(key){
  try{return window.sessionStorage.getItem(key);}catch(e){return null;}
}
function safeSessionSet(key,value){
  try{window.sessionStorage.setItem(key,value);}catch(e){}
}
function createDocSnapshot(id,data){
  return {
    id:id,
    exists:data!==undefined,
    data:function(){return deepClone(data);}
  };
}
function createQuerySnapshot(store){
  var ids=Object.keys(store||{});
  var docs=ids.map(function(id){return createDocSnapshot(id,store[id]);});
  return {
    docs:docs,
    empty:docs.length===0,
    size:docs.length,
    forEach:function(cb){docs.forEach(cb);}
  };
}
function createLocalDb(prefix){
  function storageKey(name){return prefix+name;}
  function loadCollection(name){
    var raw=safeStoreGet(storageKey(name));
    if(!raw)return {};
    try{
      var parsed=JSON.parse(raw);
      return parsed&&typeof parsed==='object'?parsed:{};
    }catch(e){
      console.warn('local collection parse failed',name,e);
      return {};
    }
  }
  function saveCollection(name,data){
    safeStoreSet(storageKey(name),JSON.stringify(data||{}));
    notify(name);
  }
  function notify(name){
    var snap=createQuerySnapshot(loadCollection(name));
    (firestoreBindings[name]&&firestoreBindings[name].localListeners||[]).slice().forEach(function(fn){
      try{fn(snap);}catch(e){console.error(e);}
    });
  }
  function docRef(name,id){
    return {
      id:id,
      async get(){
        var store=loadCollection(name);
        return createDocSnapshot(id,store[id]);
      },
      async set(data,options){
        var store=loadCollection(name);
        var next=deepClone(data)||{};
        if(options&&options.merge&&store[id]){
          store[id]=Object.assign({},store[id],next);
        }else{
          store[id]=next;
        }
        saveCollection(name,store);
      },
      async update(patch){
        var store=loadCollection(name);
        if(!store[id])throw new Error('Document does not exist: '+name+'/'+id);
        store[id]=Object.assign({},store[id],deepClone(patch)||{});
        saveCollection(name,store);
      },
      async delete(){
        var store=loadCollection(name);
        if(Object.prototype.hasOwnProperty.call(store,id)){
          delete store[id];
          saveCollection(name,store);
        }
      }
    };
  }
  return {
    collection:function(name){
      return {
        async get(){
          return createQuerySnapshot(loadCollection(name));
        },
        onSnapshot:function(onNext,_onError){
          if(!firestoreBindings[name])firestoreBindings[name]={};
          firestoreBindings[name].localListeners=firestoreBindings[name].localListeners||[];
          firestoreBindings[name].localListeners.push(onNext);
          onNext(createQuerySnapshot(loadCollection(name)));
          return function(){
            var state=firestoreBindings[name];
            if(!state||!state.localListeners)return;
            state.localListeners=state.localListeners.filter(function(fn){return fn!==onNext;});
          };
        },
        async add(data){
          var store=loadCollection(name);
          var id=uid();
          store[id]=deepClone(data)||{};
          saveCollection(name,store);
          return {id:id};
        },
        doc:function(id){
          return docRef(name,id);
        }
      };
    },
    batch:function(){
      var ops=[];
      return {
        set:function(ref,data,options){
          ops.push(function(){return ref.set(data,options);});
        },
        commit:async function(){
          for(var i=0;i<ops.length;i++) await ops[i]();
        }
      };
    }
  };
}
function serverTimestampValue(){
  return liveSyncEnabled?firebase.firestore.FieldValue.serverTimestamp():new Date().toISOString();
}
function stampToMs(value){
  if(!value)return 0;
  if(typeof value==='number')return value;
  if(typeof value==='string'){
    var parsed=Date.parse(value);
    return isNaN(parsed)?0:parsed;
  }
  if(typeof value.toDate==='function'){
    try{return value.toDate().getTime();}catch(e){}
  }
  if(typeof value.seconds==='number'){
    return value.seconds*1000+Math.floor((value.nanoseconds||0)/1000000);
  }
  if(value instanceof Date)return value.getTime();
  return 0;
}
function stampToLabel(value){
  var ms=stampToMs(value);
  return ms?new Date(ms).toLocaleDateString('ko-KR'):'';
}

if(IS_FILE_PROTOCOL){
  USING_LOCAL_DB = true;
  db = createLocalDb(LOCAL_DB_PREFIX);
}else{
  try{
    firebase.initializeApp({
      apiKey:"AIzaSyCHUqUMNlIoMIGlQr2Wv6j0ivVWXifRfXE",
      authDomain:"milsil-2403f.firebaseapp.com",
      projectId:"milsil-2403f",
      storageBucket:"milsil-2403f.firebasestorage.app",
      messagingSenderId:"957056223278",
      appId:"1:957056223278:web:dd47c88474a176e8e737bf"
    });
    db=firebase.firestore();
    db.settings({
      ignoreUndefinedProperties:true,
      merge:true
    });
    liveSyncEnabled=true;
  }catch(err){
    console.error('Firestore 초기화 실패. 로컬 저장소 모드로 전환합니다.',err);
    USING_LOCAL_DB = true;
    db = createLocalDb(LOCAL_DB_PREFIX);
  }
}

var col=db.collection('events');
var events=[];
var editId=null;
var sortOrder='asc';
var filterMode='all';
var openCards=new Set();
var openComments=new Set();

function showToast(msg){
  var t=document.getElementById('toast');
  if(!t)return;
  t.textContent=msg;t.classList.add('show');
  setTimeout(function(){t.classList.remove('show');},2200);
}
function clearFirestoreBinding(key){
  var state=firestoreBindings[key];
  if(!state)return;
  if(state.unsubscribe){
    try{state.unsubscribe();}catch(_e){}
    state.unsubscribe=null;
  }
  if(state.timer){
    clearInterval(state.timer);
    state.timer=null;
  }
}
function startPollingQuery(key,ref,onNext,onError,reason){
  var state=firestoreBindings[key]||{};
  if(state.mode==='poll')return state;
  clearFirestoreBinding(key);
  state=firestoreBindings[key]||{};
  state.mode='poll';
  state.reasonShown=!!state.reasonShown;
  firestoreBindings[key]=state;
  function fetchOnce(){
    ref.get().then(onNext).catch(function(err){
      if(onError)onError(err,true);
    });
  }
  fetchOnce();
  state.timer=window.setInterval(fetchOnce,FIRESTORE_POLL_MS);
  if(reason && !state.reasonShown){
    showToast(reason);
    state.reasonShown=true;
  }
  return state;
}
function bindQuery(key,ref,onNext,onError){
  clearFirestoreBinding(key);
  firestoreBindings[key]={mode:'live',timer:null,unsubscribe:null,reasonShown:false,localListeners:[]};
  if(USING_LOCAL_DB){
    firestoreBindings[key].mode='local';
    firestoreBindings[key].unsubscribe=ref.onSnapshot(onNext,function(err){
      if(onError)onError(err,false);
    });
    return firestoreBindings[key];
  }
  firestoreBindings[key].unsubscribe=ref.onSnapshot(onNext,function(err){
    var msg=String((err&&err.message)||err||'');
    if(/transport errored|webchannel|listen\/channel|write\/channel|(?:^|\D)400(?:\D|$)|network/i.test(msg)){
      startPollingQuery(key,ref,onNext,onError,'실시간 채널이 불안정하여 주기 동기화로 전환했습니다');
      return;
    }
    if(onError)onError(err,false);
  });
  return firestoreBindings[key];
}
window.addEventListener('beforeunload',function(){
  Object.keys(firestoreBindings).forEach(clearFirestoreBinding);
});
function applyRuntimeIndicator(){
  var rune=document.querySelector('.live-rune');
  if(!rune)return;
  if(USING_LOCAL_DB){
    rune.style.background='#f59e0b';
    rune.style.boxShadow='0 0 6px #f59e0b';
    rune.title='로컬 저장소 모드';
  }else{
    rune.title='실시간 연결됨';
  }
}
applyRuntimeIndicator();
if(USING_LOCAL_DB){
  setTimeout(function(){
    showToast(IS_FILE_PROTOCOL?'로컬 파일 모드 — 이 브라우저에만 저장됩니다':'실시간 연결 실패 — 로컬 저장소 모드로 전환했습니다');
  },300);
}
function uid(){return Date.now().toString(36)+Math.random().toString(36).slice(2);}
function esc(s){return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
function todayStr(){
  var n=new Date();
  return n.getFullYear()+'-'+String(n.getMonth()+1).padStart(2,'0')+'-'+String(n.getDate()).padStart(2,'0');
}
function isPast(d){return d<todayStr();}

function setSort(o){
  sortOrder=o;
  document.getElementById('sortAsc').className='f-btn'+(o==='asc'?' active':'');
  document.getElementById('sortDesc').className='f-btn'+(o==='desc'?' active':'');
  render();
}
function setFilter(f){
  filterMode=f;
  document.getElementById('filterAll').className='f-btn'+(f==='all'?' active':'');
  document.getElementById('filterUpcoming').className='f-btn'+(f==='upcoming'?' active':'');
  render();
}
function toggleCard(id){
  if(openCards.has(id))openCards.delete(id);else openCards.add(id);
  render();
}

function toggleForm(){
  var fc=document.getElementById('formCard');
  if(fc.classList.contains('open')){closeForm();return;}
  editId=null;
  document.getElementById('formTitleLabel').textContent='✦ 새 퀘스트 공고 ✦';
  ['fDate','fTime','fTitle','fNote','fEventPw'].forEach(function(id){
    var el=document.getElementById(id);
    if(el) el.value='';
  });
  document.getElementById('fMale').value=0;
  document.getElementById('fFemale').value=0;
  var pwEl=document.getElementById('fEventPw');
  if(pwEl) pwEl.placeholder='작성자 비밀번호 4자리';
  fc.classList.add('open');
  document.getElementById('fTitle').focus();
}
function closeForm(){
  document.getElementById('formCard').classList.remove('open');
  editId=null;
}

function validEventPassword(value){
  return /^\d{4}$/.test(String(value||'').trim());
}

async function submitEvent(){
  var date=document.getElementById('fDate').value;
  var time=document.getElementById('fTime').value;
  var title=document.getElementById('fTitle').value.trim();
  var male=parseInt(document.getElementById('fMale').value)||0;
  var female=parseInt(document.getElementById('fFemale').value)||0;
  var note=document.getElementById('fNote').value.trim();
  var eventPw=(document.getElementById('fEventPw').value||'').trim();

  if(!date||!title){alert('날짜와 작품명은 필수입니다.');return;}
  if(!editId && !validEventPassword(eventPw)){alert('공고 비밀번호 4자리를 입력해주세요.');return;}
  if(editId && eventPw && !validEventPassword(eventPw)){alert('공고 비밀번호는 4자리 숫자만 사용할 수 있습니다.');return;}

  var btn=document.getElementById('submitBtn');
  btn.textContent='게시 중...';
  btn.disabled=true;

  try{
    if(editId){
      var updateData={
        date:date,
        time:time,
        title:title,
        male:male,
        female:female,
        note:note,
        updatedAt:serverTimestampValue()
      };
      if(eventPw) updateData.authorPassword=eventPw;
      await col.doc(editId).update(updateData);
      showToast('✦ 공고가 수정되었습니다');
    }else{
      var ref=await col.add({
        date:date,
        time:time,
        title:title,
        male:male,
        female:female,
        note:note,
        authorPassword:eventPw,
        participants:[],
        comments:[],
        createdAt:serverTimestampValue()
      });
      openCards.add(ref.id);
      showToast('✦ 새 공고가 게시되었습니다');
    }
    closeForm();
  }catch(e){
    alert('저장 실패: '+e.message);
  }finally{
    btn.textContent='게시하기';
    btn.disabled=false;
  }
}

async function performDeleteEvent(id){
  if(!confirm('이 공고를 철회하시겠습니까?'))return;
  openCards.delete(id);
  await col.doc(id).delete();
  showToast('공고가 철회되었습니다');
}

function deleteEvent(id,e){
  if(e)e.stopPropagation();
  verifyEventOwner(id,function(){
    performDeleteEvent(id);
  });
}

function openEditEvent(id){
  var ev=events.find(function(x){return x.id===id;});
  if(!ev)return;
  editId=id;
  document.getElementById('formTitleLabel').textContent='✦ 공고 수정 ✦';
  document.getElementById('fDate').value=ev.date||'';
  document.getElementById('fTime').value=ev.time||'';
  document.getElementById('fTitle').value=ev.title||'';
  document.getElementById('fMale').value=ev.male||0;
  document.getElementById('fFemale').value=ev.female||0;
  document.getElementById('fNote').value=ev.note||'';
  var pwEl=document.getElementById('fEventPw');
  if(pwEl){
    pwEl.value='';
    pwEl.placeholder='새 비밀번호 입력 시 변경';
  }
  document.getElementById('formCard').classList.add('open');
  window.scrollTo({top:0,behavior:'smooth'});
}

function startEdit(id,e){
  if(e)e.stopPropagation();
  verifyEventOwner(id,function(){
    openEditEvent(id);
  });
}


async function joinEvent(id,gender,e){
  if(e)e.stopPropagation();
  var ev=events.find(function(x){return x.id===id;});if(!ev)return;
  var inputId=gender==='male'?'jnm_'+id:'jnf_'+id;
  var nameEl=document.getElementById(inputId);
  var name=nameEl?nameEl.value.trim():'';
  if(!name){
    if(nameEl){nameEl.classList.add('err');setTimeout(function(){nameEl.classList.remove('err');},1200);nameEl.focus();}
    return;
  }
  var parts=ev.participants||[];
  var gCount=parts.filter(function(p){return p.gender===gender;}).length;
  var limit=gender==='male'?ev.male:ev.female;
  if(gCount>=limit){showToast('\u26a0 '+(gender==='male'?'\ub0a8\uc790':'\uc5ec\uc790')+' \uc2ac\ub86f\uc774 \ubaa8\ub450 \ucc3c\uc2b5\ub2c8\ub2e4');return;}
  if(parts.find(function(p){return p.name===name&&p.gender===gender;})){showToast("'"+name+"'\uc740(\ub294) \uc774\ubbf8 \uc2e0\uccad\ud558\uc168\uc2b5\ub2c8\ub2e4");return;}
  var updated=parts.concat([{pid:uid(),name:name,gender:gender,time:new Date().toLocaleString('ko-KR',{month:'numeric',day:'numeric',hour:'2-digit',minute:'2-digit'})}]);
  await col.doc(id).update({participants:updated});
  nameEl.value='';
  showToast('\u2726 '+name+' \u2014 \ud30c\ud2f0 \ud569\ub958 \uc644\ub8cc!');
}

async function cancelJoin(eid,pid,e){
  if(e)e.stopPropagation();
  var ev=events.find(function(x){return x.id===eid;});if(!ev)return;
  await col.doc(eid).update({participants:(ev.participants||[]).filter(function(p){return p.pid!==pid;})});
  showToast('\ud30c\ud2f0\uc5d0\uc11c \ud0c8\ud1f4\ud558\uc600\uc2b5\ub2c8\ub2e4');
}

function toggleComments(id,e){
  if(e)e.stopPropagation();
  if(openComments.has(id))openComments.delete(id);else openComments.add(id);
  render();
}
function talkToggleLabel(cnt,open){
  return(open?'\u25b2':'\u25bc')+' \uc5ec\uad00 \uac8c\uc2dc\ud310 ('+cnt+'\uac1c '+(open?'\ub2eb\uae30':'\ubcf4\uae30')+')';
}

async function addComment(id,e){
  if(e)e.stopPropagation();
  var nm=document.getElementById('cn_'+id);
  var tx=document.getElementById('ct_'+id);
  var name=nm.value.trim(),text=tx.value.trim();
  if(!name||!text){alert('\uc774\ub984\uacfc \ub0b4\uc6a9\uc744 \uc785\ub825\ud558\uc138\uc694.');return;}
  var ev=events.find(function(x){return x.id===id;});if(!ev)return;
  var nc={cid:uid(),name:name,text:text,time:new Date().toLocaleString('ko-KR',{month:'numeric',day:'numeric',hour:'2-digit',minute:'2-digit'})};
  await col.doc(id).update({comments:(ev.comments||[]).concat([nc])});
  nm.value='';tx.value='';
  openComments.add(id);
  showToast('\u2726 \uba54\uc2dc\uc9c0\uac00 \uc804\ub2ec\ub418\uc5c8\uc2b5\ub2c8\ub2e4');
}
async function deleteComment(eid,cid,e){
  if(e)e.stopPropagation();
  var ev=events.find(function(x){return x.id===eid;});if(!ev)return;
  await col.doc(eid).update({comments:(ev.comments||[]).filter(function(c){return c.cid!==cid;})});
}

var MONTHS=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
var DAYS=['\uc77c','\uc6d4','\ud654','\uc218','\ubaa9','\uae08','\ud1a0'];
function fmtDate(s){
  if(!s)return{day:'?',mon:'?',dow:'',full:''};
  var parts=s.split('-');
  var y=parseInt(parts[0]),m=parseInt(parts[1]),d=parseInt(parts[2]);
  var obj=new Date(y,m-1,d);
  return{day:d,mon:MONTHS[m-1],dow:DAYS[obj.getDay()],full:y+'\ub144 '+m+'\uc6d4 '+d+'\uc77c'};
}
function fmtTime(t){
  if(!t)return'';
  var hp=t.split(':');var hr=parseInt(hp[0]);
  return(hr<12?'\uc624\uc804':'\uc624\ud6c4')+' '+(hr===0?12:hr>12?hr-12:hr)+':'+hp[1];
}

function render(){
  var list=document.getElementById('eventList');
  var filtered=filterMode==='upcoming'?events.filter(function(e){return !isPast(e.date);}):events;
  if(!filtered.length){
    list.innerHTML='<div class="empty"><span class="empty-icon">\ud83d\udcdc</span>'+(filterMode==='upcoming'?'\uc608\uc815\ub41c \ud000\uc2a4\ud2b8\uac00 \uc5c6\uc2b5\ub2c8\ub2e4.':'\uac8c\uc2dc\ub41c \uacf5\uace0\uac00 \uc5c6\uc2b5\ub2c8\ub2e4.')+'<br>\uc0c1\ub2e8\uc758 <b>\uacf5\uace0 \uac8c\uc2dc</b>\ub85c \uccab \ud000\uc2a4\ud2b8\ub97c \ub4f1\ub85d\ud558\uc138\uc694.</div>';
    return;
  }
  var sorted=filtered.slice().sort(function(a,b){
    var da=(a.date||'')+(a.time||''),db=(b.date||'')+(b.time||'');
    return sortOrder==='asc'?da.localeCompare(db):db.localeCompare(da);
  });
  var upcoming=sorted.filter(function(e){return !isPast(e.date);});
  var past=sorted.filter(function(e){return isPast(e.date);});
  var html='';
  if(upcoming.length){
    html+='<div class="section-head"><div class="section-head-line"></div><div class="section-head-label">\u2694 \ubaa8\uc9d1 \uacf5\uace0 \u2694</div><div class="section-head-line rev"></div></div>';
    html+=upcoming.map(function(ev){return cardHTML(ev);}).join('');
  }
  if(past.length){
    html+='<div class="section-head"><div class="section-head-line"></div><div class="section-head-label">\u263d \uc885\ub8cc\ub41c \uacf5\uace0 \u263e</div><div class="section-head-line rev"></div></div>';
    html+=past.map(function(ev){return cardHTML(ev);}).join('');
  }
  list.innerHTML=html;
}

function cardHTML(ev){
  var dt=fmtDate(ev.date);
  var tm=fmtTime(ev.time);
  var parts=ev.participants||[];
  var mP=parts.filter(function(p){return p.gender==='male';});
  var fP=parts.filter(function(p){return p.gender==='female';});
  var mFull=ev.male>0&&mP.length>=ev.male;
  var fFull=ev.female>0&&fP.length>=ev.female;
  var allFull=mFull&&fFull;
  var past=isPast(ev.date);
  var isOpen=openCards.has(ev.id);
  var cOpen=openComments.has(ev.id);
  var comments=ev.comments||[];
  var mPct=ev.male>0?Math.min(100,Math.round(mP.length/ev.male*100)):0;
  var fPct=ev.female>0?Math.min(100,Math.round(fP.length/ev.female*100)):0;
  var totalNeed=(ev.male+ev.female)-(mP.length+fP.length);
  var badge;
  if(past) badge='<span class="q-badge badge-ended">\u263d \uc784\ubb34 \uc885\ub8cc</span>';
  else if(allFull) badge='<span class="q-badge badge-full">\u2726 \ud30c\ud2f0 \uc644\uc131</span>';
  else badge='<span class="q-badge badge-active">\u2694 '+(totalNeed>0?totalNeed+'\uba85 \ubaa8\uc9d1':'\ubaa8\uc9d1\uc911')+'</span>';
  var noOne='<span class="no-member">\u2014 \uc544\uc9c1 \uc5c6\uc74c \u2014</span>';
  var mChips=mP.map(function(p){return '<span class="party-chip male">\u2694 '+esc(p.name)+'<button class="chip-x" onclick="cancelJoin(\''+ev.id+'\',\''+p.pid+'\',event)" title="\ud0c8\ud1f4">\u2715</button></span>';}).join('');
  var fChips=fP.map(function(p){return '<span class="party-chip female">\u2726 '+esc(p.name)+'<button class="chip-x" onclick="cancelJoin(\''+ev.id+'\',\''+p.pid+'\',event)" title="\ud0c8\ud1f4">\u2715</button></span>';}).join('');
  var commentsHTML=comments.length===0
    ?'<div style="font-size:12px;color:var(--ink-faint);font-style:italic;padding:4px 0">\uc544\uc9c1 \uc544\ubb34\ub3c4 \uae00\uc744 \ub0a8\uae30\uc9c0 \uc54a\uc558\uc2b5\ub2c8\ub2e4...</div>'
    :comments.map(function(c){return '<div class="talk-item"><div class="talk-avatar">'+esc(c.name).slice(0,1)+'</div><div class="talk-body"><div class="talk-meta"><span class="talk-name">'+esc(c.name)+'</span><span class="talk-time">'+esc(c.time)+'</span><button class="del-talk" onclick="deleteComment(\''+ev.id+'\',\''+c.cid+'\',event)">\uc0ad\uc81c</button></div><div class="talk-text">'+esc(c.text)+'</div></div></div>';}).join('');

  return '<div class="quest-card'+(past?' past-card':'')+(allFull&&!past?' full-card':'')+(isOpen?' open-card':'')+'" id="card_'+ev.id+'">'
    +'<div class="card-pin"></div>'
    +'<div class="card-ribbon"></div>'
    +'<div class="quest-summary" onclick="toggleCard(\''+ev.id+'\')">'
    +'<div class="date-rune"><div class="rune-day">'+dt.day+'</div><div class="rune-mon">'+dt.mon+'</div><div class="rune-dow">'+dt.dow+'</div></div>'
    +(tm?'<div class="quest-time"><svg width="10" height="10" viewBox="0 0 16 16" fill="none"><circle cx="8" cy="8" r="6.5" stroke="currentColor" stroke-width="1.2"/><path d="M8 5v3.5l2 1.2" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/></svg>'+tm+'</div>':'')
    +'<div class="quest-name">'+esc(ev.title)+'</div>'
    +'<div class="quest-right">'+badge+'<svg class="chevron-rune" width="16" height="16" viewBox="0 0 16 16" fill="none"><path d="M4 6l4 4 4-4" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg></div>'
    +'</div>'
    +'<div class="quest-detail"><div class="detail-body">'
    +(ev.note?'<div class="quest-note">'+esc(ev.note)+'</div>':'')
    +'<div class="slots-label">\ud30c\ud2f0 \uc2ac\ub86f</div>'
    +'<div class="slot-row">'
    +'<div class="slot-box male-slot"><div class="slot-top"><span class="slot-label male">\u2694 \ub0a8\uc790</span>'+(mFull?'<span class="slot-full-badge">FULL</span>':'<span class="slot-count"><b>'+mP.length+'</b> / '+ev.male+'\uba85</span>')+'</div>'
    +'<div class="slot-track"><div class="slot-fill male'+(mFull?' maxed':'')+'" style="width:'+mPct+'%"></div></div>'
    +'<div class="party-chips">'+(mChips||noOne)+'</div>'
    +(past?'<div class="ended-notice">\u2694 \uc784\ubb34\uac00 \uc885\ub8cc\ub418\uc5c8\uc2b5\ub2c8\ub2e4</div>':mFull?'<div class="ended-notice">\u2014 \uc2ac\ub86f \ub9c8\uac10 \u2014</div>':'<div class="join-field"><input type="text" id="jnm_'+ev.id+'" class="join-input" placeholder="\uc774\ub984 \uc785\ub825" onclick="event.stopPropagation()"><button class="join-btn-m" onclick="joinEvent(\''+ev.id+'\',\'male\',event)">\ucc38\uc5ec</button></div>')
    +'</div>'
    +'<div class="slot-box female-slot"><div class="slot-top"><span class="slot-label female">\u2726 \uc5ec\uc790</span>'+(fFull?'<span class="slot-full-badge">FULL</span>':'<span class="slot-count"><b>'+fP.length+'</b> / '+ev.female+'\uba85</span>')+'</div>'
    +'<div class="slot-track"><div class="slot-fill female'+(fFull?' maxed':'')+'" style="width:'+fPct+'%"></div></div>'
    +'<div class="party-chips">'+(fChips||noOne)+'</div>'
    +(past?'<div class="ended-notice">\u2726 \uc784\ubb34\uac00 \uc885\ub8cc\ub418\uc5c8\uc2b5\ub2c8\ub2e4</div>':fFull?'<div class="ended-notice">\u2014 \uc2ac\ub86f \ub9c8\uac10 \u2014</div>':'<div class="join-field"><input type="text" id="jnf_'+ev.id+'" class="join-input" placeholder="\uc774\ub984 \uc785\ub825" onclick="event.stopPropagation()"><button class="join-btn-f" onclick="joinEvent(\''+ev.id+'\',\'female\',event)">\ucc38\uc5ec</button></div>')
    +'</div></div>'
    +'<div class="tavern-talk">'
    +'<button class="talk-toggle" id="ctbtn_'+ev.id+'" onclick="toggleComments(\''+ev.id+'\',event)">'+talkToggleLabel(comments.length,cOpen)+'</button>'
    +'<div class="talk-section'+(cOpen?' open':'')+'" id="cs_'+ev.id+'">'
    +'<div class="talk-list">'+commentsHTML+'</div>'
    +'<div class="talk-input">'
    +'<input type="text" id="cn_'+ev.id+'" class="talk-name-in" placeholder="\uc774\ub984" onclick="event.stopPropagation()">'
    +'<input type="text" id="ct_'+ev.id+'" placeholder="\uba54\uc2dc\uc9c0\ub97c \ub0a8\uaca8\ubcf4\uc138\uc694..." onclick="event.stopPropagation()" onkeydown="if(event.key===\'Enter\')addComment(\''+ev.id+'\',event)">'
    +'<button class="talk-send" onclick="addComment(\''+ev.id+'\',event)">\uc804\ub2ec</button>'
    +'</div></div></div>'
    +'</div>'
    +'<div class="card-footer">'
    +'<button class="f-act" onclick="startEdit(\''+ev.id+'\',event)">\u2712 \uc218\uc815</button>'
    +'<button class="f-act danger" onclick="deleteEvent(\''+ev.id+'\');event.stopPropagation()">\u2715 \ucca0\ud68c</button>'
    +'</div></div>';
}

bindQuery('events',col,function(snap){
  events=snap.docs.map(function(d){return Object.assign({id:d.id},d.data());});
  render();
},function(err,isPolling){
  var prefix=isPolling?'⚠ 길드 데이터 동기화가 지연되고 있습니다':'⚠ 길드와의 연결이 끊겼습니다';
  document.getElementById('eventList').innerHTML='<div class="empty" style="color:#8b1a1a">'+prefix+'<br>'+esc(err&&err.message||err)+'</div>';
});



// ══════ 모험 기록부 + 회원 명부 JS ══════
var LEGACY_MEMBER_STATS = typeof _MEMBER_STATS_DATA !== 'undefined' ? _MEMBER_STATS_DATA : {};
var LEGACY_MEMBER_GAMES = {};
var STATIC_MEMBER_NAMES = typeof _MEMBERS_LIST_DATA !== 'undefined' ? _MEMBERS_LIST_DATA : [];
var LEGACY_REVIEWS = typeof _XLSX_REVIEWS_DATA !== 'undefined' ? _XLSX_REVIEWS_DATA : [];
var memberSortMode = 'exp';
var memberRoles = {};
var addedMembers = [];
var activeMemName = '';
var _rc=null,_ac={},_dc={};
var firebaseGames = [];
var firebaseReviews = [];
var gamesCol = db.collection('games');
var reviewsCol = db.collection('reviews');
var memberRolesCol = db.collection('member_roles');
var membersCol = db.collection('members');
var recEditId = null;
var acIndex = -1;

function uniqueStrings(arr){
  var seen={};
  var out=[];
  (arr||[]).forEach(function(v){
    var s=String(v||'').trim();
    if(!s||seen[s])return;
    seen[s]=true;
    out.push(s);
  });
  return out;
}
function normalizeMemberName(name){
  return String(name||'').trim().replace(/\s+/g,' ');
}
function stripMemberAlias(name){
  return normalizeMemberName(name).replace(/\s*[(（][^)）]*[)）]\s*$/,'').trim();
}
var _allMemberNamesCache=null;
var _allMemberNamesSet=null;
var _canonCache={};
function invalidateMemberNameCache(){
  _allMemberNamesCache=null;
  _allMemberNamesSet=null;
  _canonCache={};
}
function getAllMemberNames(){
  if(_allMemberNamesCache)return _allMemberNamesCache;
  _allMemberNamesCache=uniqueStrings((STATIC_MEMBER_NAMES||[]).concat(addedMembers||[])).sort(function(a,b){
    return a.localeCompare(b,'ko');
  });
  _allMemberNamesSet={};
  for(var i=0;i<_allMemberNamesCache.length;i++)_allMemberNamesSet[_allMemberNamesCache[i]]=true;
  return _allMemberNamesCache;
}
function canonicalMemberName(name){
  var raw=String(name||'');
  if(_canonCache[raw]!==undefined)return _canonCache[raw];
  var clean=normalizeMemberName(raw);
  var stripped=stripMemberAlias(clean);
  getAllMemberNames();
  var res;
  if(_allMemberNamesSet[clean])res=clean;
  else if(_allMemberNamesSet[stripped])res=stripped;
  else res=clean;
  _canonCache[raw]=res;
  return res;
}
function isRosterMember(name){
  return getAllMemberNames().indexOf(canonicalMemberName(name))>=0;
}
function starFromNumber(n){
  return n>0?['','★☆☆☆☆','★★☆☆☆','★★★☆☆','★★★★☆','★★★★★'][n]||'':'';
}
function starToNumber(str){
  return {'★☆☆☆☆':1,'★★☆☆☆':2,'★★★☆☆':3,'★★★★☆':4,'★★★★★':5}[String(str||'').trim()]||0;
}
function scoreNumber(v){
  if(v===null||v===undefined||v==='')return null;
  var n=parseFloat(v);
  return isNaN(n)?null:n;
}
function round1(n){
  return Math.round(n*10)/10;
}
function reviewKey(gameName,memberName){
  return String(gameName||'').trim()+'||'+canonicalMemberName(memberName);
}
var _mergedGamesCache=null;
var _mergedGamesByName=null;
var _playedGamesByMember=null;
var _playerRangeCache=null;
function invalidateMergedGames(){_mergedGamesCache=null;_mergedGamesByName=null;_playedGamesByMember=null;_playerRangeCache=null;}
function getMergedGames(){
  if(_mergedGamesCache)return _mergedGamesCache;
  var map={};
  (GAMES||[]).forEach(function(g){
    if(!g||!g.name)return;
    map[g.name]=Object.assign({},g);
  });
  (firebaseGames||[]).forEach(function(g){
    if(!g||!g.name)return;
    var base=map[g.name]||{};
    map[g.name]=Object.assign({},base,g,{fromFirebase:true,id:g.id});
  });
  _mergedGamesByName=map;
  _mergedGamesCache=Object.keys(map).map(function(name){return map[name];});
  return _mergedGamesCache;
}
function getGameByName(name){
  getMergedGames();
  return _mergedGamesByName?_mergedGamesByName[name]||null:null;
}
function prepareReview(raw,isFirebase){
  raw=raw||{};
  var gameName=String(raw.gameName||'').trim();
  var memberName=canonicalMemberName(raw.memberName||'');
  var game=getGameByName(gameName);
  var score=scoreNumber(raw.score);
  var gameScore=null;

  if(game&&game.scores){
    gameScore=scoreNumber(game.scores[memberName]);
    if(gameScore===null){
      gameScore=scoreNumber(game.scores[stripMemberAlias(raw.memberName||'')]);
    }
  }

  if(score===null || (!isFirebase && score===0 && gameScore!==null && gameScore!==0)){
    score=gameScore;
  }

  var difficulty=parseInt(raw.difficulty,10);
  if(isNaN(difficulty)||difficulty<1||difficulty>5){
    difficulty=starToNumber(raw.diff||'');
  }
  if(!difficulty && game && game.diff){
    difficulty=starToNumber(game.diff);
  }

  var prepared=Object.assign({},raw,{
    gameName:gameName,
    memberName:memberName,
    review:String(raw.review||'').trim(),
    score:score,
    difficulty:difficulty||0,
    diff:difficulty?starFromNumber(difficulty):(raw.diff||((game&&game.diff)||'')),
    isSpoiler:!!raw.isSpoiler,
    fromFirebase:!!isFirebase,
    isStatic:!isFirebase
  });

  return prepared;
}
var _rvByGame=null;
var _rvByMember=null;
var _liveRvByMember=null;
function invalidateReviewCache(){_rc=null;_ac={};_dc={};_rvByGame=null;_rvByMember=null;_liveRvByMember=null;invalidatePlayedByMember();}
function getReviewsByGame(){
  if(_rvByGame)return _rvByGame;
  var all=getAllReviews();
  var m={};
  for(var i=0;i<all.length;i++){
    var n=all[i].gameName;
    (m[n]=m[n]||[]).push(all[i]);
  }
  _rvByGame=m;
  return m;
}
function getReviewsFor(name){
  return getReviewsByGame()[name]||[];
}
function getReviewsByMember(){
  if(_rvByMember)return _rvByMember;
  var all=getAllReviews();
  var m={};
  for(var i=0;i<all.length;i++){
    var k=all[i].memberName;
    (m[k]=m[k]||[]).push(all[i]);
  }
  _rvByMember=m;
  return m;
}
function getLiveReviewsByMember(){
  if(_liveRvByMember)return _liveRvByMember;
  var list=firebaseReviews||[];
  var m={};
  for(var i=0;i<list.length;i++){
    var k=canonicalMemberName(list[i].memberName);
    (m[k]=m[k]||[]).push(list[i]);
  }
  _liveRvByMember=m;
  return m;
}
function getAllReviews(){
  if(_rc)return _rc;
  var legacy=(LEGACY_REVIEWS||[]).map(function(r){return prepareReview(r,false);});
  var live=(firebaseReviews||[]).map(function(r){return prepareReview(r,true);});
  var lk={};
  live.forEach(function(r){lk[reviewKey(r.gameName,r.memberName)]=true;});
  _rc=legacy.filter(function(r){return !lk[reviewKey(r.gameName,r.memberName)];}).concat(live);
  return _rc;
}
function getGameDiffNumber(n){
  if(_dc[n]!==undefined)return _dc[n];
  var rvs=getReviewsFor(n).filter(function(r){return parseInt(r.difficulty,10)>0;});
  var res;
  if(rvs.length){res=Math.max(1,Math.min(5,Math.round(rvs.reduce(function(s,r){return s+(parseInt(r.difficulty,10)||0);},0)/rvs.length)));}
  else{var g=getGameByName(n);res=(g&&g.diff)?starToNumber(g.diff):0;}
  _dc[n]=res;return res;
}
function getGameDiffString(gameName){
  return starFromNumber(getGameDiffNumber(gameName));
}
function getGameAvg(n){
  if(_ac[n]!==undefined)return _ac[n];
  var rvs=getReviewsFor(n).filter(function(r){return scoreNumber(r.score)!==null;});
  var res;
  if(rvs.length){res=round1(rvs.reduce(function(s,r){return s+scoreNumber(r.score);},0)/rvs.length);}
  else{
    var g=getGameByName(n);
    if(g&&g.scores){
      var sc=Object.keys(g.scores).map(function(m){return scoreNumber(g.scores[m]);}).filter(function(v){return v!==null;});
      if(sc.length)res=round1(sc.reduce(function(s,v){return s+v;},0)/sc.length);
    }
    if(res===undefined){var a=scoreNumber(g&&g.avg);res=a===null?0:round1(a);}
  }
  _ac[n]=res;return res;
}
function getMemberReviewEntries(member){
  var key=canonicalMemberName(member);
  return getReviewsByMember()[key]||[];
}
function getMemberPlayedGames(member){
  var key=canonicalMemberName(member);
  var playedMap={};

  getMemberReviewEntries(key).forEach(function(rv){
    var sc=scoreNumber(rv.score);
    if(sc===null)return;
    playedMap[rv.gameName]={
      name:rv.gameName,
      score:sc,
      diff:rv.diff||getGameDiffString(rv.gameName),
      avg:getGameAvg(rv.gameName),
      review:rv.review||'',
      isReviewed:true,
      createdAt:rv.createdAt||null
    };
  });

  getMergedGames().forEach(function(g){
    if(!g||!g.name||playedMap[g.name])return;
    var sc=null;
    if(g.scores){
      sc=scoreNumber(g.scores[key]);
      if(sc===null){
        sc=scoreNumber(g.scores[stripMemberAlias(key)]);
      }
    }
    if(sc===null)return;
    playedMap[g.name]={
      name:g.name,
      score:sc,
      diff:getGameDiffString(g.name)||g.diff||'',
      avg:getGameAvg(g.name),
      review:'',
      isReviewed:false,
      createdAt:null
    };
  });

  (LEGACY_MEMBER_GAMES[key]||[]).forEach(function(g){
    if(!g||!g.name||playedMap[g.name])return;
    var sc=scoreNumber(g.score);
    if(sc===null)return;
    playedMap[g.name]={
      name:g.name,
      score:sc,
      diff:g.diff||getGameDiffString(g.name),
      avg:scoreNumber(g.avg),
      review:'',
      isReviewed:false,
      createdAt:null
    };
  });

  return Object.keys(playedMap).map(function(gameName){return playedMap[gameName];});
}
var _playedByMember=null;
function invalidatePlayedByMember(){_playedByMember=null;_playedGamesByMember=null;}
function getPlayedByMember(){
  if(_playedByMember && _playedGamesByMember)return _playedByMember;
  var m={};
  var idx={};
  var games=getMergedGames();
  for(var i=0;i<games.length;i++){
    var sc=games[i].scores;
    if(!sc)continue;
    var gname=games[i].name;
    for(var k in sc){
      if(Object.prototype.hasOwnProperty.call(sc,k)){
        var canon=canonicalMemberName(k);
        m[canon]=(m[canon]||0)+1;
        (idx[canon]||(idx[canon]=new Set())).add(gname);
      }
    }
  }
  var reviews=getAllReviews();
  for(var r=0;r<reviews.length;r++){
    var rv=reviews[r];
    if(!rv||!rv.gameName||!rv.memberName)continue;
    var rcanon=canonicalMemberName(rv.memberName);
    if(!rcanon)continue;
    (idx[rcanon]||(idx[rcanon]=new Set())).add(rv.gameName);
  }
  _playedByMember=m;
  _playedGamesByMember=idx;
  return m;
}
function getPlayedGamesByMember(){
  if(!_playedGamesByMember)getPlayedByMember();
  return _playedGamesByMember;
}
function getMemberStats(member){
  var key=canonicalMemberName(member);
  var reviewEntries=getReviewsByMember()[key]||[];
  var sum=0,cnt=0;
  for(var i=0;i<reviewEntries.length;i++){
    var s=scoreNumber(reviewEntries[i].score);
    if(s!==null){sum+=s;cnt++;}
  }
  var avg=cnt?round1(sum/cnt):0;
  var liveEntries=getLiveReviewsByMember()[key]||[];
  var liveGames={};
  for(var j=0;j<liveEntries.length;j++)liveGames[liveEntries[j].gameName]=true;
  var exp=Object.keys(liveGames).length;
  var played=getPlayedByMember()[key]||0;
  return{
    exp:exp,avg:avg,played:played,reviewCount:reviewEntries.length,reviewAvg:avg
  };
}

function normalizeMemberRoles(raw){
  var normalized={};
  if(!raw||typeof raw!=='object') return normalized;
  Object.keys(raw).forEach(function(member){
    var value=raw[member];
    if(Array.isArray(value)){
      var cleaned=value.filter(function(v){return typeof v==='string'&&v.trim();}).map(function(v){return v.trim();});
      if(cleaned.length) normalized[member]=cleaned;
    }else if(typeof value==='string'&&value.trim()){
      normalized[member]=[value.trim()];
    }
  });
  return normalized;
}

function _bindMemberRolesQuery(){
  bindQuery('memberRoles',memberRolesCol,function(snap){
    var nextRoles={};
    snap.forEach(function(doc){
      var data=doc.data()||{};
      var member=canonicalMemberName(data.memberName||doc.id);
      var roles=[];
      if(Array.isArray(data.roles)){
        roles=data.roles;
      }else if(typeof data.role==='string'&&data.role.trim()){
        roles=[data.role.trim()];
      }
      roles=roles.filter(function(v){return typeof v==='string'&&v.trim();}).map(function(v){return v.trim();});
      if(member&&roles.length)nextRoles[member]=roles;
    });
    memberRoles=nextRoles;
    if((function(){var _el=document.getElementById('members-page');return _el&&_el.classList.contains('active');})() ){
      renderMemberGrid();
      renderMemberDetail();
    }
    if((function(){var _el=document.getElementById('records-page');return _el&&_el.classList.contains('active');})() ) filterGames();
  },function(err){
    console.error('member roles sync failed',err);
  });
}

async function migrateLegacyMemberRoles(){
  try{
    if(safeSessionGet('member_roles_migrated_v3')==='1') return;
    var legacyDoc=await db.collection('config').doc('memberRoles').get();
    if(!legacyDoc.exists){
      safeSessionSet('member_roles_migrated_v3','1');
      return;
    }
    var legacyRoles=normalizeMemberRoles((legacyDoc.data()||{}).roles||{});
    var members=Object.keys(legacyRoles);
    if(!members.length){
      safeSessionSet('member_roles_migrated_v3','1');
      return;
    }
    var batch=db.batch();
    members.forEach(function(member){
      batch.set(memberRolesCol.doc(member),{
        memberName:member,
        roles:legacyRoles[member],
        migratedAt:serverTimestampValue()
      },{merge:true});
    });
    await batch.commit();
    safeSessionSet('member_roles_migrated_v3','1');
  }catch(e){
    console.error('legacy member role migration failed',e);
  }
}

var _pendingRender={};
var _rafToken=0;
function _flushRenders(){
  _rafToken=0;
  var p=_pendingRender;_pendingRender={};
  if(p.stats)renderStats();
  if(p.games)filterGames();
  if(p.members)renderMemberGrid();
  if(p.memberDetail)renderMemberDetail();
  if(p.finderChips)renderFinderChips();
  if(p.finderDropdown&&_finderDropdownOpen)renderFinderDropdown();
  if(p.finderResults)debouncedRenderFinderResults();
}
function queueRender(key){
  _pendingRender[key]=true;
  if(_rafToken)return;
  var raf=window.requestAnimationFrame||function(cb){return setTimeout(cb,16);};
  _rafToken=raf(_flushRenders);
}
function _isTabActive(id){
  var el=document.getElementById(id);
  return !!(el&&el.classList.contains('active'));
}

function _bindMembersQuery(){
  bindQuery('members',membersCol,function(snap){
    addedMembers=snap.docs.map(function(doc){
      var data=doc.data()||{};
      return normalizeMemberName(data.memberName||doc.id);
    }).filter(function(name){return !!name;});
    invalidateMemberNameCache();
    invalidateReviewCache();
    invalidatePlayedByMember();
    if(_isTabActive('members-page')){
      queueRender('members');
      queueRender('memberDetail');
    }
    if(_isTabActive('findgame-page')){
      queueRender('finderChips');
      queueRender('finderDropdown');
      queueRender('finderResults');
    }
  },function(err){
    console.error('members sync failed',err);
  });
}

function _bindGamesQuery(){
  bindQuery('games',gamesCol,function(snap){
    firebaseGames=snap.docs.map(function(d){
      return Object.assign({id:d.id,fromFirebase:true},d.data());
    });
    invalidateMergedGames();
    invalidatePlayedByMember();
    if(_isTabActive('records-page')){
      queueRender('stats');
      // 열린 리뷰 폼에 입력 중이면 재렌더 스킵
      var _gl=document.getElementById('gameList');
      var _hasInput=_gl&&Array.prototype.some.call(_gl.querySelectorAll('input[type="text"],input[type="number"]'),function(el){return el.value.trim();});
      if(!_hasInput)queueRender('games');
    }
    if(_isTabActive('members-page')){
      queueRender('members');
      queueRender('memberDetail');
    }
    if(_isTabActive('findgame-page')){
      queueRender('finderChips');
      queueRender('finderDropdown');
      queueRender('finderResults');
    }
  },function(err){
    console.error('games sync failed',err);
  });
}

function _bindReviewsQuery(){
  bindQuery('reviews',reviewsCol,function(snap){
    firebaseReviews=snap.docs.map(function(d){
      return Object.assign({id:d.id,fromFirebase:true},d.data());
    });
    invalidateReviewCache();
    if(_isTabActive('records-page')){
      queueRender('stats');
      queueRender('games');
    }
    if(_isTabActive('members-page')){
      queueRender('members');
      queueRender('memberDetail');
    }
  },function(err){
    console.error('reviews sync failed',err);
  });
}

var _deferredBindingsScheduled=false;
var _deferredBindingsStarted=false;
function _startDeferredBindings(){
  if(_deferredBindingsStarted)return;
  _deferredBindingsStarted=true;
  _bindMemberRolesQuery();
  _bindMembersQuery();
  _bindGamesQuery();
  _bindReviewsQuery();
  try{migrateLegacyMemberRoles();}catch(_e){console.error('legacy migration schedule failed',_e);}
}
function _scheduleDeferredBindings(){
  if(_deferredBindingsScheduled)return;
  _deferredBindingsScheduled=true;
  var ric=window.requestIdleCallback;
  if(typeof ric==='function'){
    ric(_startDeferredBindings,{timeout:1500});
    setTimeout(_startDeferredBindings,1500);
  }else{
    setTimeout(_startDeferredBindings,300);
  }
}
if(document.readyState==='complete'||document.readyState==='interactive'){
  _scheduleDeferredBindings();
}else{
  window.addEventListener('DOMContentLoaded',_scheduleDeferredBindings,{once:true});
}
// Eager-start if user switches to a tab that needs these collections before the idle timer fires.
(function(){
  var ids=['tab-records','tab-members','tab-findgame'];
  ids.forEach(function(id){
    var btn=document.getElementById(id);
    if(btn)btn.addEventListener('click',_startDeferredBindings,{once:true});
  });
})();

function acSearch(val){
  var list=document.getElementById('acList');
  if(!val||val.length<1){list.classList.remove('open');return;}
  var q=val.toLowerCase().replace(/\s/g,'');
  var all=getMergedGames().map(function(g){return g.name;});
  var matches=all.filter(function(n){
    return n.toLowerCase().replace(/\s/g,'').indexOf(q)>=0;
  }).slice(0,8);
  if(!matches.length){list.classList.remove('open');return;}
  list.innerHTML=matches.map(function(n,i){
    var hi=n.replace(new RegExp('('+val.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')+')','gi'),'<span class="ac-match">$1</span>');
    return '<div class="ac-item" onmousedown="acSelect(\''+n.replace(/'/g,"\\'")+'\')" data-idx="'+i+'">'+hi+'</div>';
  }).join('');
  acIndex=-1;
  list.classList.add('open');
}
function acSelect(name){
  document.getElementById('rfName').value=name;
  document.getElementById('acList').classList.remove('open');
  var g=getGameByName(name);
  if(g){
    if(g.owner)document.getElementById('rfOwner').value=g.owner;
    if(g.players)document.getElementById('rfPlayers').value=g.players;
  }
}
function acKeydown(e){
  var items=document.querySelectorAll('#acList .ac-item');
  if(!items.length)return;
  if(e.key==='ArrowDown'){acIndex=Math.min(acIndex+1,items.length-1);}
  else if(e.key==='ArrowUp'){acIndex=Math.max(acIndex-1,-1);}
  else if(e.key==='Enter'){
    if(acIndex>=0){acSelect(items[acIndex].textContent);e.preventDefault();}
    return;
  }else if(e.key==='Escape'){
    document.getElementById('acList').classList.remove('open');
    return;
  }
  items.forEach(function(el,i){el.classList.toggle('selected',i===acIndex);});
}

function switchTab(tab){
  ['board','records','members','findgame'].forEach(function(t){
    var btn=document.getElementById('tab-'+t);
    if(btn)btn.className='tab-btn'+(t===tab?' active':'');
  });
  document.getElementById('board-page').classList.toggle('hidden',tab!=='board');
  document.getElementById('records-page').classList.toggle('active',tab==='records');
  document.getElementById('members-page').classList.toggle('active',tab==='members');
  var fg=document.getElementById('findgame-page');
  if(fg)fg.classList.toggle('active',tab==='findgame');
  if(tab==='records'){
    renderStats();
    filterGames();
  }
  if(tab==='members'){
    renderMemberGrid();
    renderMemberDetail();
  }
  if(tab==='findgame'){
    updateFinderTotalDisplay();
    renderFinderChips();
    renderFinderResults();
  }
}

function toggleRecForm(){
  var fc=document.getElementById('recFormCard');
  if(fc.classList.contains('open')){closeRecForm();return;}
  recEditId=null;
  document.getElementById('recFormTitle').textContent='✦ 새 게임 기록 추가 ✦';
  document.getElementById('rfName').value='';
  document.getElementById('rfOwner').value='';
  document.getElementById('rfPlayers').value='';
  fc.classList.add('open');
  document.getElementById('rfName').focus();
}
function closeRecForm(){
  document.getElementById('recFormCard').classList.remove('open');
  recEditId=null;
}
async function submitGame(){
  var name=document.getElementById('rfName').value.trim();
  if(!name){alert('게임명은 필수입니다.');return;}
  var data={
    name:name,
    owner:document.getElementById('rfOwner').value.trim(),
    players:document.getElementById('rfPlayers').value.trim(),
    updatedAt:serverTimestampValue()
  };
  var btn=document.getElementById('recSubmitBtn');
  btn.textContent='저장 중...';
  btn.disabled=true;
  try{
    if(recEditId){
      await gamesCol.doc(recEditId).update(data);
      showToast('✦ 기록이 수정되었습니다');
    }else{
      data.createdAt=serverTimestampValue();
      await gamesCol.add(data);
      showToast('✦ 기록이 추가되었습니다');
    }
    closeRecForm();
  }catch(e){
    alert('저장 실패: '+e.message);
  }finally{
    btn.textContent='기록 저장';
    btn.disabled=false;
  }
}
async function deleteGame(id,e){
  if(e)e.stopPropagation();
  if(!confirm('이 기록을 삭제하시겠습니까?'))return;
  await gamesCol.doc(id).delete();
  showToast('기록이 삭제되었습니다');
}
function editGameByCard(btn,e){
  if(e)e.stopPropagation();
  var card=btn.closest('[data-fbid]');
  if(!card)return;
  var id=card.getAttribute('data-fbid');
  var g=firebaseGames.find(function(x){return x.id===id;});
  if(!g)return;
  recEditId=id;
  document.getElementById('recFormTitle').textContent='✦ 기록 수정 ✦';
  document.getElementById('rfName').value=g.name||'';
  document.getElementById('rfOwner').value=g.owner||'';
  document.getElementById('rfPlayers').value=g.players||'';
  document.getElementById('recFormCard').classList.add('open');
  window.scrollTo({top:0,behavior:'smooth'});
}
function deleteGameByCard(btn,e){
  if(e)e.stopPropagation();
  var card=btn.closest('[data-fbid]');
  if(!card)return;
  deleteGame(card.getAttribute('data-fbid'),null);
}

function renderStats(){
  var all=getMergedGames();
  if(!all.length)return;
  var scored=all.filter(function(g){return getGameAvg(g.name)>0;});
  var best=scored.length?scored.reduce(function(a,b){
    return getGameAvg(a.name)>getGameAvg(b.name)?a:b;
  }).name:'';
  var worst=scored.length?scored.reduce(function(a,b){
    return getGameAvg(a.name)<getGameAvg(b.name)?a:b;
  }).name:'';
  document.getElementById('statRow').innerHTML=
    '<div class="stat-box"><div class="stat-val">'+all.length+'</div><div class="stat-label">총 작품 수</div></div>'+
    '<div class="stat-box"><div class="stat-val">'+getAllReviews().length+'</div><div class="stat-label">누적 리뷰</div></div>'+
    '<div class="stat-box"><div class="stat-val" style="font-size:12px;line-height:1.4">'+(best.length>8?best.slice(0,8)+'…':best||'-')+'</div><div class="stat-label">최고 평점</div></div>'+
    '<div class="stat-box"><div class="stat-val" style="font-size:12px;line-height:1.4;color:#ef4444">'+(worst.length>8?worst.slice(0,8)+'…':worst||'-')+'</div><div class="stat-label">최저 평점</div></div>';
}

function scoreClass(s){
  if(s>=9)return 's-great';
  if(s>=8)return 's-good';
  if(s>=7)return 's-mid';
  if(s>=5)return 's-low';
  return 's-bad';
}
function escR(s){
  return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

var _filterTimer=null;
function debouncedFilterGames(){
  if(_filterTimer)clearTimeout(_filterTimer);
  _filterTimer=setTimeout(function(){_filterTimer=null;filterGames();},180);
}
function filterGames(){
  var q=(document.getElementById('recSearch').value||'').trim().toLowerCase();
  var sort=document.getElementById('recSort').value;
  var diff=document.getElementById('recDiff').value;
  var score=document.getElementById('recScore').value;
  var players=document.getElementById('recPlayers').value;
  var list=getMergedGames().slice();

  if(q)list=list.filter(function(g){return g.name.toLowerCase().indexOf(q)>=0;});
  if(diff)list=list.filter(function(g){return getGameDiffString(g.name)===diff;});
  if(score==='9')list=list.filter(function(g){return getGameAvg(g.name)>=9;});
  else if(score==='8')list=list.filter(function(g){return getGameAvg(g.name)>=8;});
  else if(score==='7')list=list.filter(function(g){return getGameAvg(g.name)>=7;});
  else if(score==='5-')list=list.filter(function(g){return getGameAvg(g.name)<5&&getGameAvg(g.name)>0;});
  if(players){
    list=list.filter(function(g){
      var n=parseInt((g.players||'0').match(/\d+/)||['0'],10);
      return players==='9+'?n>=9:n===parseInt(players,10);
    });
  }

  if(sort==='avg-desc')list.sort(function(a,b){return getGameAvg(b.name)-getGameAvg(a.name);});
  else if(sort==='avg-asc')list.sort(function(a,b){return getGameAvg(a.name)-getGameAvg(b.name);});
  else if(sort==='name')list.sort(function(a,b){return a.name.localeCompare(b.name,'ko');});
  else if(sort==='diff-desc')list.sort(function(a,b){return getGameDiffNumber(b.name)-getGameDiffNumber(a.name);});
  else if(sort==='review-desc'){
    var _idx=getReviewsByGame();
    list.sort(function(a,b){
      return (_idx[b.name]?_idx[b.name].length:0)-(_idx[a.name]?_idx[a.name].length:0);
    });
  }

  renderGames(list);
}
function renderGames(list){
  var el=document.getElementById('gameList');
  if(!el)return;
  if(!list.length){
    el.innerHTML='<div class="empty"><span class="empty-icon">\ud83d\udcdc</span>\uc870\uac74\uc5d0 \ub9de\ub294 \uae30\ub85d\uc774 \uc5c6\uc2b5\ub2c8\ub2e4.</div>';
    return;
  }
  // 입력 중인 리뷰 폼 감지 - 입력값이 있으면 재렌더 스킵
  var activeInputs=el.querySelectorAll('input[type="text"],input[type="number"]');
  for(var i=0;i<activeInputs.length;i++){
    if(activeInputs[i].value.trim()){
      return; // 입력 중 → 재렌더 생략
    }
  }
  // 열린 카드/폼 상태 저장
  var openCards=[];
  var openForms=[];
  el.querySelectorAll('.game-card.open-rec').forEach(function(card){openCards.push(card.id);});
  el.querySelectorAll('.review-form.open').forEach(function(form){openForms.push(form.id);});
  el.innerHTML=list.map(function(g,i){return gameCardHTML(g,i);}).join('');
  // 열린 상태 복원
  openCards.forEach(function(id){var el2=document.getElementById(id);if(el2)el2.classList.add('open-rec');});
  openForms.forEach(function(id){var el2=document.getElementById(id);if(el2)el2.classList.add('open');});
}
function diffOptionsHTML(selected){
  var cur=parseInt(selected,10)||0;
  return '<option value="">선택</option>'
    +[1,2,3,4,5].map(function(n){
      return '<option value="'+n+'"'+(cur===n?' selected':'')+'>'+starFromNumber(n)+'</option>';
    }).join('');
}
function gameCardHTML(g,rank){
  var avg=getGameAvg(g.name);
  var diffStr=getGameDiffString(g.name);
  var _gameRvs=getReviewsFor(g.name);
  var rvCount=_gameRvs.length;
  var scoreCls=avg>0?scoreClass(avg):'s-bad';
  var rCls=rank===0?'gold':rank===1?'silver':rank===2?'bronze':'';
  var fbTag=g.fromFirebase?'<span class="firebase-tag">✦ 추가됨</span>':'';
  var editDel=g.fromFirebase
    ?'<button class="f-act" onclick="editGameByCard(this,event)">✒ 수정</button><button class="f-act danger" onclick="deleteGameByCard(this,event)">✕ 삭제</button>'
    :'';

  var rvs=_gameRvs.slice().sort(function(a,b){
    return stampToMs(b.createdAt)-stampToMs(a.createdAt);
  });

  var defaultDiffNum=starToNumber(diffStr);
  var rvHTML=rvs.length===0
    ?'<div class="no-reviews">아직 리뷰가 없습니다. 첫 번째 리뷰를 남겨보세요!</div>'
    :rvs.map(function(r){
        var sc=scoreNumber(r.score);
        var scColor=sc!==null?(sc>=9?'#4ade80':sc>=8?'#86efac':sc>=7?'#f59e0b':sc>=5?'#f97316':'#ef4444'):'#8a7050';
        var roles=getRoles(r.memberName);
        var primaryRole=roles[0]||'';
        var rClass=getRoleClass(primaryRole);
        var rColor=getRoleColor(primaryRole);
        var spoilerOpen=r.isSpoiler?'<div class="rv-spoiler-wrap spoiler-unread"><div class="rv-spoiler-overlay">&#9888; 스포일러 — 클릭하여 보기</div>':'<div>';
        return '<div class="review-item">'
          +'<div class="rv-score" style="color:'+scColor+'">'+(sc!==null?sc.toFixed(1):'-')+'</div>'
          +'<div class="rv-body">'
          +'<div class="rv-header">'
          +'<span class="rv-name '+(rClass||'')+'" style="'+(rColor?'color:'+rColor:'')+'">'+escR(r.memberName)+'</span>'
          +(primaryRole?'<span class="role-badge '+rClass+'" style="font-size:8px;padding:0 5px">'+primaryRole+'</span>':'')
          +(r.diff?'<span class="rv-time">난이도 '+escR(r.diff)+'</span>':'')
          +(r.createdAt?'<span class="rv-time">'+stampToLabel(r.createdAt)+'</span>':'')
          +(r.isStatic?'':'<button class="rv-del" data-rvid="'+r.id+'">삭제</button>')
          +'</div>'
          +spoilerOpen+(r.review?'<div class="rv-text">'+escR(r.review)+'</div>':'')+'</div>'
          +(r.isSpoiler?'<span style="font-size:9px;color:#5a4028;font-family:Cinzel,serif">탭하여 숨기기</span>':'')
          +'</div></div>';
      }).join('');

  var gname=g.name.replace(/'/g,"\\'");
  return '<div class="game-card" id="rec_'+rank+'" data-fbid="'+(g.id||'')+'">'
    +'<div class="game-summary" onclick="toggleRec('+rank+')">'
    +'<span class="rank-num '+rCls+'">'+(rank+1)+'</span>'
    +'<div class="score-ring '+scoreCls+'">'+(avg>0?avg.toFixed(1):'?')+'</div>'
    +'<div class="game-meta-col">'
    +'<div class="game-name-row"><span class="game-title">'+escR(g.name)+'</span>'+(diffStr?'<span class="diff-stars">'+escR(diffStr)+'</span>':'')+fbTag+'</div>'
    +'<div class="game-info-row">'+(g.owner?'<span class="owner-tag">✦ '+escR(g.owner)+'</span>':'')+(g.players?'<span class="g-info">'+escR(g.players)+'</span>':'')+'<span class="g-info">리뷰 '+rvCount+'개</span></div>'
    +'</div>'
    +'<div class="game-right"><svg class="chevron-rune" width="16" height="16" viewBox="0 0 16 16" fill="none"><path d="M4 6l4 4 4-4" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg></div>'
    +'</div>'
    +'<div class="game-detail">'
    +'<div class="game-review-section">'
    +'<button class="write-review-btn" onclick="toggleReviewForm(\''+gname+'\','+rank+',event)">✒ 리뷰 작성</button>'
    +'<div class="review-form" id="rvf_'+rank+'" data-gname="'+escR(g.name)+'" data-rank="'+rank+'">'
    +'<div class="review-form-row">'
    +'<div style="flex:0 0 auto"><label class="rf-label">이름</label><input type="text" class="rf-input rf-name" id="rvn_'+rank+'" placeholder="이름"></div>'
    +'<div style="flex:0 0 auto"><label class="rf-label">평점 (0~10)</label><input type="number" class="rf-input rf-score" id="rvs_'+rank+'" min="0" max="10" step="0.5" placeholder="8.5"></div>'
    +'<div style="flex:0 0 auto"><label class="rf-label">난이도</label><select class="rf-input rf-score" id="rvd_'+rank+'">'+diffOptionsHTML(defaultDiffNum)+'</select></div>'
    +'<div style="flex:1;min-width:120px"><label class="rf-label">리뷰</label><input type="text" class="rf-input rf-text" id="rvt_'+rank+'" placeholder="한줄 리뷰 (선택)"></div>'
    +'</div>'
    +'<div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px">'
    +'<label class="spoiler-toggle"><input type="checkbox" id="rvsp_'+rank+'"><span>⚠ 스포일러 포함</span></label>'
    +'<button class="rf-submit">등록</button>'
    +'</div></div>'
    +'<div class="review-list">'+rvHTML+'</div>'
    +'</div>'
    +(editDel?'<div style="display:flex;justify-content:flex-end;gap:6px;padding:8px 14px;border-top:1px solid #1e1608">'+editDel+'</div>':'')
    +'</div></div>';
}
async function toggleSpoilerOnReview(checkbox,reviewId){
  if(!reviewId)return;
  try{
    await reviewsCol.doc(reviewId).update({isSpoiler:checkbox.checked});
    showToast(checkbox.checked ? '스포일러로 설정되었습니다' : '스포일러 해제되었습니다');
  }catch(e){
    showToast('저장 실패: '+e.message);
    checkbox.checked=!checkbox.checked;
  }
}
function toggleRec(rank){
  var card=document.getElementById('rec_'+rank);
  if(!card)return;
  if(card.classList.contains('open-rec'))card.classList.remove('open-rec');
  else card.classList.add('open-rec');
}

document.addEventListener('keydown',function(e){
  if(e.key!=='Enter')return;
  var el=e.target;
  if(!el.classList.contains('rf-text'))return;
  var form=el.closest('.review-form');
  if(!form)return;
  var rank=form.getAttribute('data-rank');
  var gname=form.getAttribute('data-gname');
  if(rank!==null&&gname)submitReview(gname,parseInt(rank,10),null);
});
document.addEventListener('click',function(e){
  var sp=e.target.closest('.rv-spoiler-wrap');
  if(sp){sp.classList.toggle('revealed');return;}
  var delBtn=e.target.closest('[data-rvid]');
  if(delBtn){deleteReview(delBtn.getAttribute('data-rvid'),e);return;}
  var roleBtn=e.target.closest('[data-assign-member]');
  if(roleBtn){
    var rm=roleBtn.getAttribute('data-assign-member');
    var rr=roleBtn.getAttribute('data-assign-role');
    if(rr)toggleRole(rm,rr);
    else clearAllRoles(rm);
    return;
  }
  var panelBtn=e.target.closest('[data-toggle-panel]');
  if(panelBtn){toggleRolePanel(panelBtn.getAttribute('data-toggle-panel'));return;}
  var memberAddBtn=e.target.closest('[data-toggle-member-admin]');
  if(memberAddBtn){toggleMemberAdminPanel();return;}
  var memberSaveBtn=e.target.closest('[data-add-member]');
  if(memberSaveBtn){addMember();return;}
  var rfBtn=e.target.closest('.rf-submit');
  if(rfBtn){
    var form=rfBtn.closest('.review-form');
    if(form){
      var rank=form.getAttribute('data-rank');
      var gname=form.getAttribute('data-gname');
      if(rank!==null&&gname)submitReview(gname,parseInt(rank,10),null);
    }
    return;
  }
  var memBtn=e.target.closest('[data-mname]');
  if(memBtn){showMember(memBtn.getAttribute('data-mname'));return;}
  var closeBtn=e.target.closest('.member-detail-close');
  if(closeBtn){
    activeMemName='';
    renderMemberGrid();
    document.getElementById('memberDetail').classList.remove('open');
    return;
  }
});
function toggleReviewForm(gname,rank,e){
  if(e)e.stopPropagation();
  var f=document.getElementById('rvf_'+rank);
  if(f)f.classList.toggle('open');
}
async function submitReview(gameName,rank,e){
  if(e)e.stopPropagation();
  var rawName=(document.getElementById('rvn_'+rank).value||'').trim();
  var name=canonicalMemberName(rawName);
  var score=scoreNumber(document.getElementById('rvs_'+rank).value);
  var diffNum=parseInt(document.getElementById('rvd_'+rank).value,10)||0;
  var review=(document.getElementById('rvt_'+rank).value||'').trim();
  var spEl=document.getElementById('rvsp_'+rank);
  var isSpoiler=spEl?spEl.checked:false;

  if(!rawName){alert('이름을 입력해주세요.');return;}
  if(score===null||score<0||score>10){alert('0~10 사이의 평점을 입력해주세요.');return;}
  if(diffNum<1||diffNum>5){alert('난이도를 선택해주세요.');return;}

  var existing=getAllReviews().find(function(rv){
    return reviewKey(rv.gameName,rv.memberName)===reviewKey(gameName,name);
  });
  var existedBefore=!!existing;
  var payload={
    gameName:gameName,
    memberName:name,
    score:score,
    difficulty:diffNum,
    diff:starFromNumber(diffNum),
    review:review,
    isSpoiler:isSpoiler,
    updatedAt:serverTimestampValue()
  };

  try{
    if(existing&&existing.fromFirebase&&existing.id){
      await reviewsCol.doc(existing.id).update(payload);
    }else{
      payload.createdAt=serverTimestampValue();
      await reviewsCol.add(payload);
    }

    document.getElementById('rvn_'+rank).value='';
    document.getElementById('rvs_'+rank).value='';
    document.getElementById('rvd_'+rank).value='';
    document.getElementById('rvt_'+rank).value='';
    if(spEl)spEl.checked=false;
    var f=document.getElementById('rvf_'+rank);
    if(f)f.classList.remove('open');

    var toastMsg='✦ '+name+' 님의 리뷰가 등록되었습니다';
    if(isRosterMember(name)&&!existedBefore)toastMsg+=' · EXP +1';
    else if(isRosterMember(name)&&existedBefore)toastMsg+=' · 기존 작품 리뷰로 EXP 유지';
    else toastMsg+=' · 명부 미등록';
    showToast(toastMsg);
  }catch(err){
    alert('리뷰 저장 실패: '+err.message);
  }
}
async function deleteReview(id,e){
  if(e)e.stopPropagation();
  if(!confirm('리뷰를 삭제하시겠습니까?'))return;
  await reviewsCol.doc(id).delete();
  showToast('리뷰가 삭제되었습니다');
}

function getLevel(exp){ return Math.floor((exp||0)/5); }
function getLvColor(lv){
  if(lv>=14)return'#f59e0b';
  if(lv>=10)return'#c9a84c';
  if(lv>=7)return'#86efac';
  if(lv>=4)return'#93c5fd';
  if(lv>=2)return'#aaa';
  return'#4a3820';
}
function getRoleClass(roleOrArr){
  var role=Array.isArray(roleOrArr)?(roleOrArr[0]||''):roleOrArr;
  if(role==='마스터플레이어')return'role-master';
  if(role==='수사관')return'role-detective';
  if(role==='연기자')return'role-actor';
  if(role==='명탐정')return'role-genius';
  if(role==='페르소나')return'role-persona';
  return'';
}
function getRoleColor(roleOrArr){
  var role=Array.isArray(roleOrArr)?(roleOrArr[0]||''):roleOrArr;
  if(role==='마스터플레이어')return'#ef4444';
  if(role==='수사관')return'#7dd3fc';
  if(role==='연기자')return'#f9a8d4';
  if(role==='명탐정')return'#1d4ed8';
  if(role==='페르소나')return'#a855f7';
  return'';
}
function getRoleBadgesHTML(roleOrArr){
  var roles=Array.isArray(roleOrArr)?roleOrArr:(roleOrArr?[roleOrArr]:[]);
  if(!roles.length)return'';
  return roles.map(function(r){
    return '<span class="role-badge '+getRoleClass(r)+'" style="margin-right:3px">'+r+'</span>';
  }).join('');
}

var isAdminUnlocked=false;
var ADMIN_PW='4521';
var pwContext={mode:'admin',eventId:'',onSuccess:null};

function openPwModal(options){
  options=options||{};
  pwContext={
    mode:options.mode||'admin',
    eventId:options.eventId||'',
    onSuccess:typeof options.onSuccess==='function'?options.onSuccess:null
  };
  document.getElementById('pwTitle').textContent=options.title||(pwContext.mode==='event'?'✦ 작성자 비밀번호 ✦':'✦ 관리자 인증 ✦');
  document.getElementById('pwInput').value='';
  document.getElementById('pwError').textContent='';
  document.getElementById('pwModalWrap').classList.remove('hidden');
  setTimeout(function(){document.getElementById('pwInput').focus();},100);
}
function closePwModal(){
  document.getElementById('pwModalWrap').classList.add('hidden');
  pwContext={mode:'admin',eventId:'',onSuccess:null};
}
function pwKeyInput(el){
  if(el.value.length===4)confirmPw();
}
function confirmPw(){
  var val=(document.getElementById('pwInput').value||'').trim();
  if(!/^\d{4}$/.test(val)){
    document.getElementById('pwError').textContent='4자리 숫자를 입력해주세요';
    return;
  }

  if(pwContext.mode==='admin'){
    if(val===ADMIN_PW){
      var cb=pwContext.onSuccess;
      isAdminUnlocked=true;
      closePwModal();
      showToast('✦ 관리자 모드 활성화');
      if(cb)cb();
    }else{
      document.getElementById('pwError').textContent='비밀번호가 틀렸습니다';
      document.getElementById('pwInput').value='';
    }
    return;
  }

  if(pwContext.mode==='event'){
    var ev=events.find(function(x){return x.id===pwContext.eventId;});
    if(ev&&String(ev.authorPassword||'')===val){
      var done=pwContext.onSuccess;
      closePwModal();
      if(done)done();
    }else{
      document.getElementById('pwError').textContent='작성자 비밀번호가 틀렸습니다';
      document.getElementById('pwInput').value='';
    }
  }
}

function toggleAdminMode(){
  if(isAdminUnlocked){
    isAdminUnlocked=false;
    document.getElementById('adminToggleBtn').classList.remove('admin-active');
    showToast('\uad00\ub9ac\uc790 \ubaa8\ub4dc \ud574\uc81c');
    renderMemberGrid();
    return;
  }
  requireAdmin(function(){
    document.getElementById('adminToggleBtn').classList.add('admin-active');
    renderMemberGrid();
    showToast('\u2726 \uad00\ub9ac\uc790 \ubaa8\ub4dc \ud65c\uc131\ud654');
  });
}
function requireAdmin(onSuccess){
  if(isAdminUnlocked){
    if(typeof onSuccess==='function')onSuccess();
    return true;
  }
  openPwModal({mode:'admin',onSuccess:onSuccess});
  return false;
}
function verifyEventOwner(id,onSuccess){
  var ev=events.find(function(x){return x.id===id;});
  if(!ev)return false;
  if(isAdminUnlocked){
    if(typeof onSuccess==='function')onSuccess();
    return true;
  }
  if(!ev.authorPassword){
    showToast('이 공고는 관리자만 수정·삭제할 수 있습니다');
    return false;
  }
  openPwModal({mode:'event',eventId:id,onSuccess:onSuccess});
  return false;
}

function getRoles(member){
  var key=canonicalMemberName(member);
  var roles=memberRoles[key];
  if(!roles)return [];
  return Array.isArray(roles)?roles.filter(function(v){return typeof v==='string'&&v.trim();}).map(function(v){return v.trim();}):[];
}
function getPrimaryRole(member){
  var roles=getRoles(member);
  return roles.length?roles[0]:'';
}
function openRolePanel(member){
  var panel=document.getElementById('rp_'+member);
  if(panel)panel.classList.add('open');
}
async function assignRole(member,role){
  member=canonicalMemberName(member);
  if(!isAdminUnlocked){
    requireAdmin(function(){assignRole(member,role);});
    return;
  }
  if(!member)return;
  var ref=memberRolesCol.doc(member);
  if(!role){
    await ref.delete().catch(function(){});
    showToast(member+' 역할 해제');
    return;
  }
  await ref.set({
    memberName:member,
    roles:[role],
    updatedAt:serverTimestampValue()
  },{merge:true});
  showToast(member+' → '+role);
}
function toggleRolePanel(member){
  member=canonicalMemberName(member);
  if(!isAdminUnlocked){
    requireAdmin(function(){toggleRolePanel(member);});
    return;
  }
  var panel=document.getElementById('rp_'+member);
  if(panel)panel.classList.toggle('open');
}
async function toggleRole(member,role){
  member=canonicalMemberName(member);
  if(!isAdminUnlocked){
    requireAdmin(function(){toggleRole(member,role);});
    return;
  }
  if(!member||!role)return;
  var ref=memberRolesCol.doc(member);
  var currentRoles=getRoles(member);
  var nextRoles=currentRoles.indexOf(role)>=0
    ?currentRoles.filter(function(r){return r!==role;})
    :currentRoles.concat([role]);

  if(!nextRoles.length){
    await ref.delete().catch(function(){});
    showToast(member+' 역할 해제');
    return;
  }

  await ref.set({
    memberName:member,
    roles:nextRoles,
    updatedAt:serverTimestampValue()
  },{merge:true});
  showToast(member+' → '+nextRoles.join(', '));
}
async function clearAllRoles(member){
  member=canonicalMemberName(member);
  if(!isAdminUnlocked){
    requireAdmin(function(){clearAllRoles(member);});
    return;
  }
  if(!member)return;
  await memberRolesCol.doc(member).delete().catch(function(){});
  showToast(member+' 모든 역할 해제');
}

function toggleMemberAdminPanel(){
  if(!isAdminUnlocked){
    requireAdmin(function(){toggleMemberAdminPanel();});
    return;
  }
  var panel=document.getElementById('memberAdminPanel');
  if(!panel)return;
  panel.classList.toggle('open');
  if(panel.classList.contains('open')){
    var input=document.getElementById('newMemberName');
    if(input)input.focus();
  }
}
async function addMember(){
  if(!isAdminUnlocked){
    requireAdmin(function(){addMember();});
    return;
  }
  var input=document.getElementById('newMemberName');
  if(!input)return;
  var name=normalizeMemberName(input.value);
  if(!name){alert('회원 이름을 입력해주세요.');return;}
  if(getAllMemberNames().indexOf(name)>=0){
    showToast('이미 등록된 회원입니다');
    return;
  }
  try{
    await membersCol.doc(name).set({
      memberName:name,
      createdAt:serverTimestampValue()
    },{merge:true});
    input.value='';
    showToast('✦ '+name+' 회원이 추가되었습니다');
  }catch(e){
    alert('회원 추가 실패: '+e.message);
  }
}
async function removeMember(name){
  if(!isAdminUnlocked){
    requireAdmin(function(){removeMember(name);});
    return;
  }
  if(!confirm('\''+name+'\' 회원을 명부에서 삭제하시겠습니까?\n(리뷰 기록은 유지됩니다)'))return;
  try{
    await membersCol.doc(name).delete();
    if(activeMemName===name){
      activeMemName='';
      document.getElementById('memberDetail').classList.remove('open');
    }
    showToast(name+' 회원이 삭제되었습니다');
  }catch(e){
    alert('삭제 실패: '+e.message);
  }
}


function setMemberSort(mode){
  memberSortMode=mode;
  ['exp','avg','name'].forEach(function(m){
    var el=document.getElementById('msort-'+m);
    if(el)el.className='ms-btn'+(m===mode?' active':'');
  });
  renderMemberGrid();
}
var _memberTimer=null;
function debouncedRenderMemberGrid(){
  if(_memberTimer)clearTimeout(_memberTimer);
  _memberTimer=setTimeout(function(){_memberTimer=null;renderMemberGrid();},180);
}
function renderMemberGrid(){
  var q=(document.getElementById('memberSearch').value||'').trim();
  var grid=document.getElementById('memberGrid');
  var list=getAllMemberNames();
  if(q)list=list.filter(function(m){return m.indexOf(q)>=0;});
  var statsCache={};
  list.forEach(function(name){statsCache[name]=getMemberStats(name);});
  function gs(name){return statsCache[name]||{exp:0,avg:0,played:0,reviewCount:0};}
  if(memberSortMode==='exp')list.sort(function(a,b){return gs(b).exp-gs(a).exp;});
  else if(memberSortMode==='avg')list.sort(function(a,b){return gs(b).avg-gs(a).avg;});
  else list.sort(function(a,b){return a.localeCompare(b,'ko');});
  var adminSection=isAdminUnlocked?'<div style="grid-column:1/-1"><button class="rec-add-btn" style="width:100%;justify-content:center;font-size:11px;padding:6px" onclick="addMember()">+ \ud68c\uc6d0 \ucd94\uac00</button></div>':'';
  var html=list.map(function(m){
    var s=gs(m);
    var lv=getLevel(s.exp);
    var lvColor=getLvColor(lv);
    var nextLvExp=(lv+1)*5;var curLvExp=lv*5;
    var pct=s.exp>0?Math.min(100,Math.round((s.exp-curLvExp)/(nextLvExp-curLvExp)*100)):0;
    var roles=getRoles(m);
    var primaryRole=roles[0]||'';
    var roleColor=getRoleColor(primaryRole);
    var isActive=m===activeMemName;
    return '<button class="member-card-btn'+(isActive?' active-member':'')+'" data-mname="'+m+'">'
      +'<div class="member-avatar" style="'+(roleColor?'border-color:'+roleColor+'60;background:linear-gradient(135deg,'+roleColor+'20,'+roleColor+'10)':'')+'">'+m.slice(0,1)+'</div>'
      +'<span style="font-size:12px;font-weight:600;'+(roleColor?'color:'+roleColor:'')+'">'+m+'</span>'
      +getRoleBadgesHTML(roles)
      +'<div style="display:flex;align-items:center;gap:4px;margin-top:1px">'
      +'<span class="lv-badge" style="color:'+lvColor+';border-color:'+lvColor+'40;background:'+lvColor+'15">Lv.'+lv+'</span>'
      +'<span style="font-size:9px;color:#6a5028">EXP '+s.exp+'</span>'
      +'</div>'
      +'<div class="lv-bar-wrap"><div class="lv-bar-fill" style="width:'+pct+'%;background:'+lvColor+'"></div></div>'
      +(s.avg>0?'<span class="member-avg-val">avg '+s.avg.toFixed(1)+'</span>':'')
      +(s.reviewCount>0?'<span class="member-count">\ub9ac\ubdf0 '+s.reviewCount+'\uac1c</span>':(s.played>0?'<span class="member-count">\ud50c\ub808\uc774 '+s.played+'\ud68c</span>':''))
      +(isAdminUnlocked?'<button class="member-del-btn" data-remove-member="'+m+'" title="\ud68c\uc6d0 \uc0ad\uc81c">\u2715</button>':'')
      +'</button>';
  }).join('');
  grid.innerHTML=adminSection+html;
}
function showMember(name){
  activeMemName=canonicalMemberName(name);
  renderMemberGrid();
  renderMemberDetail();
}
function renderMemberDetail(){
  var det=document.getElementById('memberDetail');
  if(!activeMemName){det.classList.remove('open');return;}
  var m=canonicalMemberName(activeMemName);
  if(getAllMemberNames().indexOf(m)<0){det.classList.remove('open');return;}

  var s=getMemberStats(m);
  var lv=getLevel(s.exp);
  var lvColor=getLvColor(lv);
  var nextLvExp=(lv+1)*5;
  var curLvExp=lv*5;
  var pct=s.exp>0?Math.min(100,Math.round((s.exp-curLvExp)/(nextLvExp-curLvExp)*100)):0;
  var roles=getRoles(m);
  var primaryRole=roles[0]||'';
  var roleColor=getRoleColor(primaryRole);
  var rvs=getMemberReviewEntries(m).slice().sort(function(a,b){
    var aTime=stampToMs(a.createdAt);
    var bTime=stampToMs(b.createdAt);
    if(bTime!==aTime)return bTime-aTime;
    return (scoreNumber(b.score)||0)-(scoreNumber(a.score)||0);
  });
  var playedGames=getMemberPlayedGames(m).slice().sort(function(a,b){
    return (scoreNumber(b.score)||0)-(scoreNumber(a.score)||0);
  });

  var ROLES=['마스터플레이어','수사관','연기자','명탐정','페르소나'];
  var roleBtns=ROLES.map(function(r){
    var rClass=getRoleClass(r);
    var rColor=getRoleColor(r);
    var isCur=roles.indexOf(r)>=0;
    var bg=isCur?rColor+'20':'transparent';
    return '<button class="role-assign-btn" data-assign-member="'+m+'" data-assign-role="'+r+'" style="color:'+rColor+';border-color:'+rColor+'50;background:'+bg+'">'+(isCur?'✓ ':'')+r+'</button>';
  }).join('');

  var rvHTML=rvs.length===0
    ?'<div class="mg-empty">아직 작성한 리뷰가 없습니다.</div>'
    :rvs.map(function(rv){
        var sc=scoreNumber(rv.score);
        var scColor=sc!==null?(sc>=9?'#4ade80':sc>=8?'#86efac':sc>=7?'#f59e0b':sc>=5?'#f97316':'#ef4444'):'#8a7050';
        return '<div class="member-game-row">'
          +'<div class="mg-score" style="color:'+scColor+'">'+(sc!==null?sc.toFixed(1):'-')+'</div>'
          +'<div class="mg-info"><div class="mg-name">'+escR(rv.gameName)+'</div>'
          +(rv.diff?'<div class="mg-diff">난이도 '+escR(rv.diff)+'</div>':'')
          +(rv.review?'<div class="mg-review">'+escR(rv.review)+'</div>':'')
          +'</div>'
          +(rv.createdAt?'<div class="mg-date">'+stampToLabel(rv.createdAt)+'</div>':'')
          +'</div>';
      }).join('');

  var csvHTML=playedGames.length===0
    ?'<div class="mg-empty">플레이 기록이 없습니다.</div>'
    :playedGames.map(function(g){
        var sc=scoreNumber(g.score);
        var scColor=sc!==null?(sc>=9?'#4ade80':sc>=8?'#86efac':sc>=7?'#f59e0b':sc>=5?'#f97316':'#ef4444'):'#8a7050';
        return '<div class="member-game-row">'
          +'<div class="mg-score" style="color:'+scColor+'">'+(sc!==null?sc.toFixed(1):'-')+'</div>'
          +'<div class="mg-info"><div class="mg-name">'+escR(g.name)+'</div>'+(g.diff?'<div class="mg-diff">'+escR(g.diff)+'</div>':'')+'</div>'
          +'</div>';
      }).join('');

  det.innerHTML=
    '<div class="member-detail-header">'
    +'<div class="member-avatar-lg" style="'+(roleColor?'border-color:'+roleColor+';background:linear-gradient(135deg,'+roleColor+'30,'+roleColor+'10)':'')+'">'+m.slice(0,1)+'</div>'
    +'<div style="flex:1;min-width:0">'
    +'<div style="font-family:Cinzel,serif;font-size:18px;font-weight:700;'+(roleColor?'color:'+roleColor:'color:var(--gold)')+'">'+m+'</div>'
    +(roles.length?'<div style="margin-top:4px">'+getRoleBadgesHTML(roles)+'</div>':'')
    +'<div style="display:flex;align-items:center;gap:8px;margin-top:6px">'
    +'<span class="lv-badge" style="color:'+lvColor+';border-color:'+lvColor+'50;background:'+lvColor+'20;font-size:12px;padding:3px 10px">Lv.'+lv+'</span>'
    +'<span style="font-size:11px;color:#8a7050">EXP '+s.exp+' · 다음 레벨까지 '+(nextLvExp-s.exp)+' EXP</span>'
    +'</div>'
    +'<div class="lv-bar-wrap" style="margin-top:5px;max-width:200px"><div class="lv-bar-fill" style="width:'+pct+'%;background:'+lvColor+'"></div></div>'
    +'<div class="member-stats" style="margin-top:8px">'
    +(s.avg>0?'<div class="mstat"><div class="mstat-avg" style="color:'+lvColor+'">'+s.avg.toFixed(1)+'</div><div class="mstat-label">평균 평점</div></div>':'')
    +'<div class="mstat"><div class="mstat-val">'+s.played+'</div><div class="mstat-label">플레이 기록</div></div>'
    +'<div class="mstat"><div class="mstat-val">'+s.exp+'</div><div class="mstat-label">EXP</div></div>'
    +(s.reviewCount?'<div class="mstat"><div class="mstat-val">'+s.reviewCount+'</div><div class="mstat-label">리뷰 수</div></div>':'')
    +'</div></div>'
    +'<div style="display:flex;flex-direction:column;gap:6px;align-items:flex-end;flex-shrink:0">'
    +'<button class="f-act member-detail-close">닫기</button>'
    +(isAdminUnlocked?'<button class="f-act danger" data-remove-member="'+m+'">✕ 삭제</button>':'')
    +'<button class="assign-role-btn" data-toggle-panel="'+m+'">✦ 역할 부여</button>'
    +'</div></div>'
    +'<div class="role-select-panel" id="rp_'+m+'">'
    +'<div class="role-select-title">역할 선택</div>'
    +'<div class="role-btns">'+roleBtns+'<button class="role-assign-btn none-btn" data-assign-member="'+m+'" data-assign-role="">모두 해제</button></div>'
    +'</div>'
    +(rvs.length?'<div style="margin-top:12px;padding-top:12px;border-top:1px solid #2a1e08"><div class="sbar-label" style="margin-bottom:8px">내가 남긴 리뷰</div><div class="member-games-list">'+rvHTML+'</div></div>':'')
    +'<div style="margin-top:12px;padding-top:12px;border-top:1px solid #2a1e08">'
    +'<div class="sbar-label" style="margin-bottom:8px">플레이 기록 ('+playedGames.length+'게임 · 높은 평점순)</div>'
    +'<div class="member-games-list">'+csvHTML+'</div>'
    +'</div>';
  det.classList.add('open');
}


// ══════ 게임 찾기 탭 ══════
var selectedFinderMembers=new Set();
var _finderUnknownOpen=false;
var _finderTotal=1;
var _finderDropdownOpen=false;

function parsePlayersField(text){
  var raw=String(text||'').trim();
  if(!raw)return null;
  var t=raw.replace(/\s+/g,'');
  var m;
  // "9인 이상", "9명 이상"
  m=t.match(/^(\d+)(?:인|명)이상$/);
  if(m)return {min:parseInt(m[1],10),max:Infinity};
  // "3인 이하", "3명 이하"
  m=t.match(/^(\d+)(?:인|명)이하$/);
  if(m)return {min:1,max:parseInt(m[1],10)};
  // "6~7명", "4-6명", "4~6인"
  m=t.match(/^(\d+)[~-](\d+)(?:인|명)?$/);
  if(m){
    var lo=parseInt(m[1],10),hi=parseInt(m[2],10);
    if(lo>hi){var tmp=lo;lo=hi;hi=tmp;}
    return {min:lo,max:hi};
  }
  // "4명", "5인"
  m=t.match(/^(\d+)(?:인|명)$/);
  if(m){var n=parseInt(m[1],10);return {min:n,max:n};}
  return null;
}
function getPlayerRange(gameName,playersText){
  if(!_playerRangeCache)_playerRangeCache=new Map();
  if(_playerRangeCache.has(gameName))return _playerRangeCache.get(gameName);
  var res=parsePlayersField(playersText);
  if(res===null && String(playersText||'').trim()){
    console.warn('[finder] players 파싱 실패:',gameName,playersText);
  }
  _playerRangeCache.set(gameName,res);
  return res;
}

function updateFinderTotalDisplay(){
  var el=document.getElementById('finderTotalVal');
  if(el)el.textContent=String(_finderTotal);
}
function adjustFinderTotal(delta){
  var next=_finderTotal+delta;
  var floor=Math.max(1,selectedFinderMembers.size);
  if(next<floor)next=floor;
  if(next===_finderTotal)return;
  _finderTotal=next;
  updateFinderTotalDisplay();
  debouncedRenderFinderResults();
}

function renderFinderChips(){
  var el=document.getElementById('finderChipList');
  if(!el)return;
  var list=Array.from(selectedFinderMembers);
  list.sort(function(a,b){return a.localeCompare(b,'ko');});
  el.innerHTML=list.map(function(canon){
    var roles=getRoles(canon);
    var primaryRole=roles[0]||'';
    var roleColor=getRoleColor(primaryRole);
    var color=roleColor||'var(--gold)';
    return '<span class="finder-chip" style="border-color:'+color+'aa;color:'+color+'">'
      +'<span class="finder-chip-name">'+escR(canon)+'</span>'
      +'<button type="button" class="finder-chip-remove" data-finder-remove="'+escR(canon)+'" aria-label="'+escR(canon)+' 제거">×</button>'
      +'</span>';
  }).join('');
  var sumEl=document.getElementById('finderSummary');
  if(sumEl)sumEl.textContent='선택된 회원 '+selectedFinderMembers.size+'명';
}

function _finderCandidates(query){
  var q=String(query||'').trim().toLowerCase();
  var all=getAllMemberNames();
  var seen={};
  var cands=[];
  for(var i=0;i<all.length;i++){
    var raw=all[i];
    var canon=canonicalMemberName(raw);
    if(!canon||canon==='미참여 인원')continue;
    if(seen[canon])continue;
    seen[canon]=1;
    if(selectedFinderMembers.has(canon))continue;
    if(q && canon.toLowerCase().indexOf(q)<0 && raw.toLowerCase().indexOf(q)<0)continue;
    cands.push(canon);
  }
  cands.sort(function(a,b){return a.localeCompare(b,'ko');});
  return cands;
}
function renderFinderDropdown(){
  var listEl=document.getElementById('finderDropdownList');
  if(!listEl)return;
  var input=document.getElementById('finderDropdownInput');
  var q=input?input.value:'';
  var cands=_finderCandidates(q);
  if(cands.length===0){
    listEl.innerHTML='<div class="finder-dropdown-empty">일치하는 회원 없음</div>';
    return;
  }
  listEl.innerHTML=cands.map(function(canon){
    var roles=getRoles(canon);
    var primaryRole=roles[0]||'';
    var roleColor=getRoleColor(primaryRole)||'#c9a84c';
    return '<button type="button" class="finder-dropdown-item" data-finder-add="'+escR(canon)+'">'
      +'<span class="finder-dropdown-dot" style="background:'+roleColor+'"></span>'
      +'<span style="color:'+roleColor+'">'+escR(canon)+'</span>'
      +'</button>';
  }).join('');
}
function openFinderDropdown(){
  _finderDropdownOpen=true;
  var listEl=document.getElementById('finderDropdownList');
  if(listEl)listEl.removeAttribute('hidden');
  renderFinderDropdown();
}
function closeFinderDropdown(){
  if(!_finderDropdownOpen)return;
  _finderDropdownOpen=false;
  var listEl=document.getElementById('finderDropdownList');
  if(listEl)listEl.setAttribute('hidden','');
}
function handleFinderDropdownInput(){
  if(!_finderDropdownOpen)openFinderDropdown();
  else renderFinderDropdown();
}

function addFinderMember(name){
  var canon=canonicalMemberName(name);
  if(!canon||canon==='미참여 인원')return;
  if(selectedFinderMembers.has(canon))return;
  selectedFinderMembers.add(canon);
  if(selectedFinderMembers.size>_finderTotal){
    _finderTotal=selectedFinderMembers.size;
    updateFinderTotalDisplay();
  }
  var input=document.getElementById('finderDropdownInput');
  if(input){input.value='';input.focus();}
  renderFinderChips();
  if(_finderDropdownOpen)renderFinderDropdown();
  debouncedRenderFinderResults();
}
function removeFinderMember(name){
  var canon=canonicalMemberName(name);
  if(!selectedFinderMembers.has(canon))return;
  selectedFinderMembers.delete(canon);
  renderFinderChips();
  if(_finderDropdownOpen)renderFinderDropdown();
  debouncedRenderFinderResults();
}
function clearFinderSelection(){
  var hadAny=selectedFinderMembers.size>0;
  selectedFinderMembers.clear();
  _finderTotal=1;
  updateFinderTotalDisplay();
  renderFinderChips();
  if(_finderDropdownOpen)renderFinderDropdown();
  if(hadAny)renderFinderResults();
}
function toggleFinderUnknown(){
  _finderUnknownOpen=!_finderUnknownOpen;
  var sec=document.getElementById('finderUnknownSection');
  var body=document.getElementById('finderUnknownBody');
  if(sec)sec.classList.toggle('open',_finderUnknownOpen);
  if(body){
    if(_finderUnknownOpen)body.removeAttribute('hidden');
    else body.setAttribute('hidden','');
  }
}

function _finderRangeSortKey(range,dir){
  if(!range)return dir==='asc'?Infinity:-Infinity;
  if(dir==='asc')return range.min;
  return range.max===Infinity?1e9:range.max;
}
function renderFinderResults(){
  var listEl=document.getElementById('finderList');
  var unknownSec=document.getElementById('finderUnknownSection');
  var unknownBody=document.getElementById('finderUnknownBody');
  var unknownLabel=document.getElementById('finderUnknownLabel');
  if(!listEl)return;

  var N=_finderTotal;
  var selCount=selectedFinderMembers.size;
  var qEl=document.getElementById('finderSearch');
  var sortEl=document.getElementById('finderSort');
  var query=qEl?(qEl.value||'').trim().toLowerCase():'';
  var sortKey=sortEl?sortEl.value:'game-asc';

  var excluded=new Set();
  if(selCount>0){
    var idx=getPlayedGamesByMember();
    selectedFinderMembers.forEach(function(m){
      var s=idx[m];
      if(s)s.forEach(function(g){excluded.add(g);});
    });
  }

  var normal=[],unknown=[];
  var games=getMergedGames();
  for(var i=0;i<games.length;i++){
    var g=games[i];
    if(!g||!g.name)continue;
    if(excluded.has(g.name))continue;
    var range=getPlayerRange(g.name,g.players);
    if(range===null){unknown.push({game:g,range:null});}
    else if(range.min<=N && N<=range.max){normal.push({game:g,range:range});}
  }

  if(query){
    normal=normal.filter(function(x){return x.game.name.toLowerCase().indexOf(query)>=0;});
    unknown=unknown.filter(function(x){return x.game.name.toLowerCase().indexOf(query)>=0;});
  }

  var sorter;
  if(sortKey==='game-desc')sorter=function(a,b){return b.game.name.localeCompare(a.game.name,'ko');};
  else if(sortKey==='players-asc')sorter=function(a,b){return _finderRangeSortKey(a.range,'asc')-_finderRangeSortKey(b.range,'asc');};
  else if(sortKey==='players-desc')sorter=function(a,b){return _finderRangeSortKey(b.range,'desc')-_finderRangeSortKey(a.range,'desc');};
  else sorter=function(a,b){return a.game.name.localeCompare(b.game.name,'ko');};
  normal.sort(sorter);
  unknown.sort(function(a,b){return a.game.name.localeCompare(b.game.name,'ko');});

  if(normal.length===0){
    if(query)listEl.innerHTML='<div class="finder-empty-state">\''+escR(query)+'\' 검색 결과 없음. 검색어를 바꾸거나 인원을 조정하세요.</div>';
    else if(selCount>0)listEl.innerHTML='<div class="finder-empty-state">총 '+N+'명(회원 '+selCount+'명 선택)이 함께 즐길 수 있는 미플레이 게임이 없습니다.</div>';
    else listEl.innerHTML='<div class="finder-empty-state">총 '+N+'명이 즐길 수 있는 게임이 없습니다.</div>';
  }else{
    listEl.innerHTML=normal.map(function(x){
      var p=x.game.players||'';
      return '<div class="finder-game-row">'
        +'<div class="finder-game-name">'+escR(x.game.name)+'</div>'
        +(p?'<div class="finder-game-players">'+escR(p)+'</div>':'')
        +'</div>';
    }).join('');
  }

  if(unknown.length>0 && unknownSec){
    unknownSec.removeAttribute('hidden');
    if(unknownLabel)unknownLabel.textContent='⚠ 인원 표기 불명 ('+unknown.length+')';
    if(unknownBody){
      unknownBody.innerHTML=unknown.map(function(x){
        var p=x.game.players||'(표기 없음)';
        return '<div class="finder-game-row">'
          +'<div class="finder-game-name">'+escR(x.game.name)+'</div>'
          +'<div class="finder-game-players">'+escR(p)+'</div>'
          +'</div>';
      }).join('');
    }
  }else if(unknownSec){
    unknownSec.setAttribute('hidden','');
  }
}
var _finderTimer=null;
function debouncedRenderFinderResults(){
  if(_finderTimer)clearTimeout(_finderTimer);
  _finderTimer=setTimeout(function(){_finderTimer=null;renderFinderResults();},180);
}

document.addEventListener('click',function(e){
  var addBtn=e.target.closest('[data-finder-add]');
  if(addBtn){
    addFinderMember(addBtn.getAttribute('data-finder-add'));
    return;
  }
  var rmBtn=e.target.closest('[data-finder-remove]');
  if(rmBtn){
    removeFinderMember(rmBtn.getAttribute('data-finder-remove'));
    return;
  }
  if(_finderDropdownOpen){
    var wrap=document.getElementById('finderDropdownWrap');
    if(wrap && !wrap.contains(e.target))closeFinderDropdown();
  }
});

window._finder={
  caches:function(){return {
    _playedByMember:_playedByMember,
    _playedGamesByMember:_playedGamesByMember,
    _playerRangeCache:_playerRangeCache,
    selectedFinderMembers:Array.from(selectedFinderMembers),
    _finderTotal:_finderTotal
  };},
  selected:function(){return Array.from(selectedFinderMembers);},
  total:function(){return _finderTotal;},
  explain:function(gameName){
    var g=getMergedGames().find(function(x){return x.name===gameName;});
    if(!g)return {error:'not found'};
    var range=getPlayerRange(gameName,g.players);
    var N=_finderTotal;
    var selCount=selectedFinderMembers.size;
    var playedBy=[];
    var idx=getPlayedGamesByMember();
    selectedFinderMembers.forEach(function(m){
      if(idx[m]&&idx[m].has(gameName))playedBy.push(m);
    });
    return {
      name:gameName,
      playersText:g.players,
      range:range,
      total:N,
      selectedCount:selCount,
      rangeOk:range?(N>=range.min&&N<=range.max):'unknown',
      playedBy:playedBy,
      verdict:playedBy.length?'excluded(played)'
        :!range?'unknown-bucket'
        :(N<range.min||N>range.max)?'excluded(range)'
        :'included'
    };
  }
};


