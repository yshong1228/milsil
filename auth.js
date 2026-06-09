// ── 밀실극장 Firebase Auth ──
// app.js 이후 로드됨. firebase.initializeApp() 완료 후 실행.

(function(){
  'use strict';

  if(window.USING_LOCAL_DB || window.IS_FILE_PROTOCOL){
    var ov=document.getElementById('loginOverlay');
    if(ov) ov.style.display='none';
    window.signInWithGoogle=function(){};
    window.doSignOut=function(){};
    return;
  }

  var auth, db;
  try{
    auth=firebase.auth();
    db=firebase.firestore();
  }catch(e){
    console.error('[Auth] 초기화 실패:', e);
    var ov2=document.getElementById('loginOverlay');
    if(ov2) ov2.style.display='none';
    return;
  }

  // JSON 정적 목록 (app.js가 미리 로드) + '미참여 인원' 제외
  function getJsonMemberNames(){
    return (window._MEMBERS_LIST_DATA||[]).filter(function(n){
      return n && n!=='미참여 인원';
    });
  }

  // ── 헤더 프로필 업데이트 ──
  function updateHeaderProfile(user, memberName){
    var profile=document.getElementById('userProfile');
    var avatarImg=document.getElementById('userAvatarImg');
    var nameEl=document.getElementById('userNameText');
    if(profile) profile.style.display='flex';
    if(avatarImg){
      avatarImg.src=user.photoURL||'';
      avatarImg.style.display=user.photoURL?'block':'none';
    }
    if(nameEl){
      var label=memberName||user.displayName||user.email||'';
      nameEl.textContent=label.length>8?label.substring(0,8)+'…':label;
      nameEl.title=label;
    }
  }

  // ── 미연결 회원 배너 ──
  // JSON 전체 목록 기준으로, Firestore에 uid가 없는 회원을 표시
  function checkUnlinkedMembers(){
    var jsonNames=getJsonMemberNames();
    if(!jsonNames.length) return;

    db.collection('members').get().then(function(snap){
      var linkedSet={};
      snap.forEach(function(doc){
        var d=doc.data();
        if(d.uid) linkedSet[d.memberName||doc.id]=true;
      });

      var unlinked=jsonNames.filter(function(n){ return !linkedSet[n]; });

      var banner=document.getElementById('unlinkedBanner');
      var countEl=document.getElementById('unlinkedBannerCount');
      var namesEl=document.getElementById('unlinkedBannerNames');
      if(!banner) return;

      if(unlinked.length>0){
        if(countEl) countEl.textContent=unlinked.length;
        if(namesEl) namesEl.textContent=unlinked.join(', ');
        banner.style.display='block';
      }else{
        banner.style.display='none';
      }
    }).catch(function(e){
      console.error('[Auth] 미연결 확인 실패:',e);
    });
  }

  window.dismissUnlinkedBanner=function(){
    var banner=document.getElementById('unlinkedBanner');
    if(banner) banner.style.display='none';
  };

  // ── 본인 확인 모달 표시 ──
  // JSON 목록을 기반으로 회원을 나열하고, Firestore로 UID 연결 여부만 확인
  function showLinkModal(user){
    var modal=document.getElementById('linkModal');
    if(!modal) return;

    var nameEl=document.getElementById('linkGoogleName');
    var avatarEl=document.getElementById('linkGoogleAvatar');
    if(nameEl) nameEl.textContent=user.displayName||user.email||'';
    if(avatarEl && user.photoURL){
      avatarEl.src=user.photoURL;
      avatarEl.style.display='block';
    }

    var grid=document.getElementById('linkMemberGrid');
    if(grid) grid.innerHTML='<div style="text-align:center;padding:1rem;color:#a09070;font-family:Cinzel,serif;font-size:12px;letter-spacing:1px">☽ 명부 불러오는 중... ☾</div>';

    var confirmBtn=document.getElementById('linkConfirmBtn');
    if(confirmBtn){ confirmBtn.disabled=true; confirmBtn.textContent='이름을 선택해주세요'; }

    modal.style.display='flex';

    var jsonNames=getJsonMemberNames();

    if(!jsonNames.length){
      if(grid) grid.innerHTML='<div style="text-align:center;padding:1.5rem;color:#a09070;font-size:12px">등록된 회원이 없습니다<br>관리자에게 문의해주세요</div>';
      return;
    }

    // Firestore에서 이미 연결된 이름 목록만 가져옴
    db.collection('members').get().then(function(snap){
      var linkedSet={};
      snap.forEach(function(doc){
        var d=doc.data();
        if(d.uid) linkedSet[d.memberName||doc.id]=true;
      });

      if(!grid) return;
      grid.innerHTML='';

      // JSON 목록 기준으로 렌더 (가나다 정렬은 JSON 파일 순서 그대로)
      jsonNames.forEach(function(name){
        var taken=!!linkedSet[name];

        var btn=document.createElement('button');
        btn.className='link-member-btn'+(taken?' link-member-taken':'');
        btn.setAttribute('data-name',name);
        btn.disabled=taken;
        btn.innerHTML='<span class="link-member-name">'+name+'</span>'
          +(taken?'<span class="link-taken-badge">연결됨</span>':'');

        if(!taken){
          btn.onclick=function(){
            document.querySelectorAll('.link-member-btn').forEach(function(b){ b.classList.remove('selected'); });
            btn.classList.add('selected');
            if(confirmBtn){
              confirmBtn.disabled=false;
              confirmBtn.textContent='✦  '+name+'으로 연결하기';
            }
          };
        }
        grid.appendChild(btn);
      });
    }).catch(function(e){
      console.error('[Auth] 연결 상태 로딩 실패:',e);
      // Firestore 실패해도 JSON 목록은 전부 선택 가능하게
      if(!grid) return;
      grid.innerHTML='';
      jsonNames.forEach(function(name){
        var btn=document.createElement('button');
        btn.className='link-member-btn';
        btn.setAttribute('data-name',name);
        btn.innerHTML='<span class="link-member-name">'+name+'</span>';
        btn.onclick=function(){
          document.querySelectorAll('.link-member-btn').forEach(function(b){ b.classList.remove('selected'); });
          btn.classList.add('selected');
          if(confirmBtn){
            confirmBtn.disabled=false;
            confirmBtn.textContent='✦  '+name+'으로 연결하기';
          }
        };
        grid.appendChild(btn);
      });
    });
  }

  // ── 연결 확정 ──
  window.confirmMemberLink=function(){
    var selected=document.querySelector('.link-member-btn.selected');
    if(!selected) return;
    var memberName=selected.getAttribute('data-name');
    var user=auth.currentUser;
    if(!user||!memberName) return;

    var confirmBtn=document.getElementById('linkConfirmBtn');
    if(confirmBtn){ confirmBtn.disabled=true; confirmBtn.textContent='연결 중...'; }

    // 선점 여부 재확인 후 저장
    db.collection('members').doc(memberName).get().then(function(doc){
      if(doc.exists && doc.data().uid){
        if(confirmBtn){ confirmBtn.disabled=false; confirmBtn.textContent='✦  '+memberName+'으로 연결하기'; }
        if(typeof showToast==='function') showToast('이미 다른 계정이 연결된 이름입니다');
        selected.disabled=true;
        selected.classList.remove('selected');
        selected.classList.add('link-member-taken');
        selected.innerHTML='<span class="link-member-name">'+memberName+'</span><span class="link-taken-badge">연결됨</span>';
        if(confirmBtn) confirmBtn.textContent='이름을 선택해주세요';
        return Promise.reject('taken');
      }

      var batch=db.batch();
      // users/{uid} 생성
      batch.set(db.collection('users').doc(user.uid),{
        memberName:memberName,
        email:user.email||'',
        displayName:user.displayName||'',
        photoURL:user.photoURL||'',
        linkedAt:firebase.firestore.FieldValue.serverTimestamp()
      });
      // members/{name} — JSON 전용 회원은 문서가 없으므로 set+merge로 생성
      batch.set(db.collection('members').doc(memberName),{
        memberName:memberName,
        uid:user.uid,
        email:user.email||''
      },{merge:true});

      return batch.commit();
    }).then(function(){
      var modal=document.getElementById('linkModal');
      if(modal) modal.style.display='none';
      window.currentLinkedMember=memberName;
      updateHeaderProfile(user,memberName);
      if(typeof showToast==='function') showToast('✦  '+memberName+'님 연결 완료');
      checkUnlinkedMembers();
    }).catch(function(e){
      if(e==='taken') return;
      console.error('[Auth] 연결 실패:',e);
      if(confirmBtn){ confirmBtn.disabled=false; confirmBtn.textContent='✦  '+memberName+'으로 연결하기'; }
      if(typeof showToast==='function') showToast('연결 실패: '+e.message);
    });
  };

  // ── Auth 상태 변경 감지 ──
  auth.onAuthStateChanged(function(user){
    window.currentUser=user;
    var overlay=document.getElementById('loginOverlay');
    var profile=document.getElementById('userProfile');
    var linkModal=document.getElementById('linkModal');

    if(user){
      if(overlay) overlay.style.display='none';

      db.collection('users').doc(user.uid).get().then(function(doc){
        if(doc.exists){
          var data=doc.data();
          window.currentLinkedMember=data.memberName||null;
          updateHeaderProfile(user,data.memberName||null);
          if(linkModal) linkModal.style.display='none';
          // 이미 연결된 사용자에게도 미연결 배너 표시
          checkUnlinkedMembers();
        }else{
          // 기존 로그인 사용자 포함 — users 문서 없으면 본인 확인 필수
          if(profile) profile.style.display='none';
          showLinkModal(user);
        }
      }).catch(function(e){
        console.error('[Auth] users 조회 실패:',e);
        updateHeaderProfile(user,null);
      });
    }else{
      if(overlay) overlay.style.display='flex';
      if(profile) profile.style.display='none';
      if(linkModal) linkModal.style.display='none';
      var banner=document.getElementById('unlinkedBanner');
      if(banner) banner.style.display='none';
      window.currentLinkedMember=null;
    }
  });

  // ── Google 로그인 ──
  window.signInWithGoogle=function(){
    var provider=new firebase.auth.GoogleAuthProvider();
    provider.setCustomParameters({hl:'ko'});
    auth.signInWithPopup(provider).catch(function(err){
      console.error('[Auth] Google 로그인 실패:',err);
      var msg;
      switch(err.code){
        case 'auth/popup-closed-by-user': msg='로그인이 취소되었습니다';break;
        case 'auth/popup-blocked': msg='팝업이 차단되었습니다. 팝업 허용 후 다시 시도해주세요';break;
        case 'auth/network-request-failed': msg='네트워크 오류가 발생했습니다';break;
        default: msg='로그인에 실패했습니다';
      }
      if(typeof showToast==='function') showToast(msg);
    });
  };

  // ── 로그아웃 ──
  window.doSignOut=function(){
    auth.signOut().then(function(){
      if(typeof showToast==='function') showToast('로그아웃 되었습니다');
    }).catch(function(err){
      console.error('[Auth] 로그아웃 실패:',err);
    });
  };

})();
